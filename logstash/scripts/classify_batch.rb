# frozen_string_literal: true

# Hybrid classify filter:
# - Metadata hit (sourcetype/source rules from classify_rules.json): classify locally.
#   First time a data_stream is seen → sync POST /ensure/batch; then cache in @ensured.
# - Metadata miss → buffer and POST /classify/batch (message-pattern / generic path).
#
# script_params: classify_url, batch_size, flush_ms, max_buffer, max_egress,
#                message_prefix_bytes, data_stream_namespace, rules_path

def register(params)
  require "json"
  require "net/http"
  require "set"
  require "thread"
  require "uri"

  @classify_url = (params["classify_url"] || "http://classify:8080").to_s.sub(%r{/+$}, "")
  @batch_url = "#{@classify_url}/classify/batch"
  @ensure_url = "#{@classify_url}/ensure/batch"
  @batch_size = (params["batch_size"] || 100).to_i
  @batch_size = 100 if @batch_size < 1
  @flush_ms = (params["flush_ms"] || 200).to_i
  @flush_ms = 200 if @flush_ms < 1
  @max_buffer = (params["max_buffer"] || 5000).to_i
  @max_buffer = [@max_buffer, @batch_size].max
  @max_egress = (params["max_egress"] || @max_buffer).to_i
  @max_egress = [@max_egress, @batch_size].max
  @message_prefix_bytes = (params["message_prefix_bytes"] || 512).to_i
  @message_prefix_bytes = 512 if @message_prefix_bytes < 1
  @namespace = (params["data_stream_namespace"] || "default").to_s
  @namespace = "default" if @namespace.empty?
  @rules_path = (params["rules_path"] || "/usr/share/logstash/scripts/classify_rules.json").to_s

  load_metadata_rules!(@rules_path)

  @buffer = []
  @batch_started_at = nil
  @buffer_mutex = Mutex.new
  @buffer_cv = ConditionVariable.new

  @batch_uri = URI.parse(@batch_url)
  @ensure_uri = URI.parse(@ensure_url)
  @http = Net::HTTP.new(@batch_uri.host, @batch_uri.port)
  @http.open_timeout = 5
  @http.read_timeout = 30
  @http.keep_alive_timeout = 60
  @http.start
  @http_mutex = Mutex.new

  @ensured = Set.new
  @ensured_mutex = Mutex.new

  @egress = []
  @egress_mutex = Mutex.new
  @egress_cv = ConditionVariable.new
  @stop = false
  @flusher = Thread.new { flush_loop }
end

def load_metadata_rules!(path)
  raw = JSON.parse(File.read(path))
  @access_sourcetype_re = Regexp.new(raw.fetch("access_sourcetype"), Regexp::IGNORECASE)
  @syslog_sourcetype_re = Regexp.new(raw.fetch("syslog_sourcetype"), Regexp::IGNORECASE)
  @access_source_re = Regexp.new(raw.fetch("access_source"), Regexp::IGNORECASE)
  @syslog_source_re = Regexp.new(raw.fetch("syslog_source"), Regexp::IGNORECASE)
  @pipeline_template = raw.fetch("pipeline_name_template").to_s
rescue StandardError => e
  raise "classify_batch failed to load rules from #{path}: #{e.class}: #{e.message}"
end

def close
  @stop = true
  begin
    @flusher&.wakeup
  rescue ThreadError
    nil
  end
  @buffer_mutex.synchronize { @buffer_cv.broadcast }
  @egress_mutex.synchronize { @egress_cv.broadcast }
  @flusher&.join(2)

  loop do
    leftover = nil
    @buffer_mutex.synchronize do
      leftover = take_batch_locked! unless @buffer.empty?
    end
    break unless leftover

    push_egress(classify_events(leftover))
  end

  dropped = 0
  @egress_mutex.synchronize { dropped = @egress.length }
  if dropped.positive?
    log_error("classify_batch close: #{dropped} classified event(s) not re-injected (no final flush)")
  end

  begin
    @http.finish if @http&.started?
  rescue StandardError
    nil
  end
end

def log_error(message)
  return unless defined?(@logger) && @logger

  @logger.error(message)
rescue StandardError
  nil
end

def filter(event)
  tags = event.get("tags")
  if tags.is_a?(Array) && tags.include?("_classify_tick")
    event.cancel
    return flush_aged_and_drain
  end

  local = classify_from_metadata(event)
  if local
    apply_local_hit!(event, local)
    out = [event]
    out.concat(drain_egress)
    return out
  end

  batch = nil
  accepted = false
  @buffer_mutex.synchronize do
    while !@stop && @buffer.length >= @max_buffer
      @buffer_cv.wait(@buffer_mutex, 0.05)
    end

    unless @stop
      @buffer << event
      @batch_started_at ||= monotonic_ms
      event.cancel
      accepted = true
      batch = take_batch_locked! if @buffer.length >= @batch_size || buffer_aged?
    end
  end

  unless accepted
    apply_result(event, fallback_result)
    return [event]
  end

  out = []
  out.concat(classify_events(batch)) if batch
  out.concat(drain_egress)
  out
end

def flush(options = {})
  final = false
  if options.respond_to?(:[])
    final = options[:final] || options["final"]
  end
  final = final == true || final.to_s == "true"

  out = []
  if final
    loop do
      batch = nil
      @buffer_mutex.synchronize do
        batch = take_batch_locked! unless @buffer.empty?
      end
      break unless batch

      out.concat(classify_events(batch))
    end
  else
    batch = nil
    @buffer_mutex.synchronize do
      batch = take_batch_locked! if !@buffer.empty? && buffer_aged?
    end
    out.concat(classify_events(batch)) if batch
  end
  out.concat(drain_egress)
  out
end

def flush_aged_and_drain
  flush({})
end

def flush_loop
  interval = @flush_ms / 1000.0
  until @stop
    sleep(interval)
    break if @stop

    batch = nil
    @buffer_mutex.synchronize do
      batch = take_batch_locked! if !@buffer.empty? && (@buffer.length >= @batch_size || buffer_aged?)
    end
    next unless batch

    finished = classify_events(batch)
    push_egress(finished)
  end
rescue StandardError => e
  log_error("classify_batch flusher died: #{e.class}: #{e.message}")
end

def monotonic_ms
  Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000.0
rescue StandardError
  Time.now.to_f * 1000.0
end

def buffer_aged?
  return false if @buffer.empty? || @batch_started_at.nil?

  (monotonic_ms - @batch_started_at) >= @flush_ms
end

def take_batch_locked!
  return nil if @buffer.empty?

  if @buffer.length <= @batch_size
    events = @buffer
    @buffer = []
    @batch_started_at = nil
  else
    events = @buffer.shift(@batch_size)
  end
  @buffer_cv.broadcast
  events
end

def push_egress(events)
  return if events.nil? || events.empty?

  @egress_mutex.synchronize do
    while !@stop && (@egress.length + events.length) > @max_egress && !@egress.empty?
      @egress_cv.wait(@egress_mutex, 0.05)
    end
    @egress.concat(events)
  end
end

def drain_egress
  out = []
  @egress_mutex.synchronize do
    return [] if @egress.empty?

    out = @egress
    @egress = []
    @egress_cv.broadcast
  end
  out
end

def strip_splunk_prefix(value)
  s = value.to_s
  %w[host:: source:: sourcetype::].each do |prefix|
    return s[prefix.length..] if s.start_with?(prefix)
  end
  s
end

def pipeline_name_for(kind)
  @pipeline_template.gsub("{kind}", kind.to_s.tr("_", "-"))
end

# Returns a result hash or nil when message-path classify is required.
def classify_from_metadata(event)
  sourcetype = strip_splunk_prefix(event.get("sourcetype")).downcase
  source = strip_splunk_prefix(event.get("source")).downcase
  index = event.get("splunk_index").to_s.strip

  kind = nil
  reason = nil
  if !sourcetype.empty? && sourcetype.match?(@access_sourcetype_re)
    kind = "access_log"
    reason = "sourcetype=#{sourcetype.inspect}"
  elsif !sourcetype.empty? && sourcetype.match?(@syslog_sourcetype_re)
    kind = "syslog"
    reason = "sourcetype=#{sourcetype.inspect}"
  elsif !source.empty? && source.match?(@access_source_re)
    kind = "access_log"
    reason = "source=#{source.inspect}"
  elsif !source.empty? && source.match?(@syslog_source_re)
    kind = "syslog"
    reason = "source=#{source.inspect}"
  else
    return nil
  end

  dataset = index.empty? ? kind : "#{index}.#{kind}"
  {
    "kind" => kind,
    "dataset" => dataset,
    "namespace" => @namespace,
    "data_stream" => "logs-#{dataset}-#{@namespace}",
    "pipeline_name" => pipeline_name_for(kind),
    "reason" => reason,
    "fallback" => false
  }
end

def stream_ensured?(name)
  @ensured_mutex.synchronize { @ensured.include?(name) }
end

def mark_ensured!(name)
  return if name.nil? || name.empty?

  @ensured_mutex.synchronize { @ensured.add(name) }
end

def apply_local_hit!(event, result)
  stream = result["data_stream"].to_s
  if stream_ensured?(stream)
    apply_result(event, result)
    return
  end

  ensure_result = post_ensure([stream]).first
  if ensure_result && (ensure_result["ok"] == true || ensure_result["ok"].to_s == "true")
    resolved = ensure_result["resolved_stream"].to_s
    resolved = stream if resolved.empty?
    mark_ensured!(resolved)
    apply_result(event, result.merge("data_stream" => resolved))
  else
    resolved = if ensure_result
                 ensure_result["resolved_stream"].to_s
               else
                 ""
               end
    resolved = "logs-generic-#{@namespace}" if resolved.empty?
    mark_ensured!(resolved)
    apply_result(
      event,
      fallback_result.merge(
        "data_stream" => resolved,
        "namespace" => @namespace,
        "reason" => "fallback=ensure_failed"
      )
    )
  end
end

def classify_events(events)
  return [] if events.nil? || events.empty?

  payloads = events.map { |e| build_payload(e) }
  results = post_classify_batch(payloads)
  events.each_with_index do |e, i|
    rearm_event!(e)
    apply_result(e, results[i])
    stream = e.get("[@metadata][target_stream]").to_s
    mark_ensured!(stream) unless stream.empty?
  end
  events
end

def build_payload(event)
  sourcetype = event.get("sourcetype").to_s
  source = event.get("source").to_s
  payload = {
    "sourcetype" => sourcetype,
    "source" => source,
    "splunk_index" => event.get("splunk_index").to_s
  }
  # Metadata miss path: always send a short message prefix for pattern classify.
  message = event.get("message").to_s
  payload["message"] = if message.bytesize > @message_prefix_bytes
                          message.byteslice(0, @message_prefix_bytes)
                        else
                          message
                        end
  payload
end

def post_classify_batch(payloads)
  req = Net::HTTP::Post.new(@batch_uri.request_uri)
  req["Content-Type"] = "application/json"
  req["Connection"] = "keep-alive"
  req.body = JSON.generate("events" => payloads)

  resp = @http_mutex.synchronize do
    ensure_http_started!
    @http.request(req)
  end

  unless resp.is_a?(Net::HTTPSuccess)
    log_error("classify_batch HTTP #{resp.code}: #{resp.body.to_s[0, 500]}")
    return Array.new(payloads.length) { fallback_result }
  end
  parsed = JSON.parse(resp.body)
  results = parsed["results"]
  if !results.is_a?(Array) || results.length != payloads.length
    log_error("classify_batch unexpected results length")
    return Array.new(payloads.length) { fallback_result }
  end
  results
rescue StandardError => e
  log_error("classify_batch failed: #{e.class}: #{e.message}")
  begin
    @http_mutex.synchronize { restart_http! }
  rescue StandardError
    nil
  end
  Array.new(payloads.length) { fallback_result }
end

def post_ensure(streams)
  req = Net::HTTP::Post.new(@ensure_uri.request_uri)
  req["Content-Type"] = "application/json"
  req["Connection"] = "keep-alive"
  req.body = JSON.generate("streams" => streams)

  resp = @http_mutex.synchronize do
    ensure_http_started!
    @http.request(req)
  end

  unless resp.is_a?(Net::HTTPSuccess)
    log_error("ensure_batch HTTP #{resp.code}: #{resp.body.to_s[0, 500]}")
    return streams.map do |s|
      {
        "data_stream" => s,
        "ok" => false,
        "fallback" => true,
        "resolved_stream" => "logs-generic-#{@namespace}"
      }
    end
  end
  parsed = JSON.parse(resp.body)
  results = parsed["results"]
  return [] unless results.is_a?(Array)

  results
rescue StandardError => e
  log_error("ensure_batch failed: #{e.class}: #{e.message}")
  begin
    @http_mutex.synchronize { restart_http! }
  rescue StandardError
    nil
  end
  streams.map do |s|
    {
      "data_stream" => s,
      "ok" => false,
      "fallback" => true,
      "resolved_stream" => "logs-generic-#{@namespace}"
    }
  end
end

def ensure_http_started!
  return if @http.started?

  @http.start
end

def restart_http!
  begin
    @http.finish if @http.started?
  rescue StandardError
    nil
  end
  @http = Net::HTTP.new(@batch_uri.host, @batch_uri.port)
  @http.open_timeout = 5
  @http.read_timeout = 30
  @http.keep_alive_timeout = 60
  @http.start
end

def fallback_result
  {
    "kind" => "generic",
    "dataset" => "generic",
    "namespace" => @namespace,
    "data_stream" => "logs-generic-#{@namespace}",
    "pipeline_name" => pipeline_name_for("generic"),
    "reason" => "fallback=batch_error",
    "fallback" => true
  }
end

def rearm_event!(event)
  return unless event.respond_to?(:cancelled?) && event.cancelled?

  if event.respond_to?(:uncancel)
    event.uncancel
  else
    event.instance_variable_set(:@cancelled, false)
  end
end

def apply_result(event, result)
  result = fallback_result if result.nil? || !result.is_a?(Hash)
  stream = result["data_stream"].to_s
  stream = "logs-generic-#{@namespace}" if stream.empty?
  kind = result["kind"].to_s
  kind = "generic" if kind.empty?
  dataset = result["dataset"].to_s
  dataset = "generic" if dataset.empty?
  namespace = result["namespace"].to_s
  namespace = @namespace if namespace.empty?

  event.set("[@metadata][target_stream]", stream)
  event.set("[event][kind]", kind)
  event.set("[event][dataset]", dataset)
  event.set("[data_stream][type]", "logs")
  event.set("[data_stream][dataset]", dataset)
  event.set("[data_stream][namespace]", namespace)
  event.set("[splunk][pipeline]", result["pipeline_name"].to_s)
  event.set("[splunk][classify_reason]", result["reason"].to_s)
  event.set("[splunk][index]", event.get("splunk_index").to_s)

  is_fallback = result["fallback"] == true || result["fallback"].to_s == "true"
  if is_fallback
    tags = event.get("tags") || []
    tags = [tags] unless tags.is_a?(Array)
    tags << "_classify_failed" unless tags.include?("_classify_failed")
    event.set("tags", tags)
  end
end
