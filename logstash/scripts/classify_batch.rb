# frozen_string_literal: true

# Buffer events and classify via POST /classify/batch.
# script_params: classify_url, batch_size, flush_ms
#
# - Reuses one Net::HTTP keep-alive connection
# - Takes batches under the buffer mutex; HTTP runs outside that lock
# - A flusher thread wakes every flush_ms so idle buffers don't wait on
#   Logstash's coarser periodic_flush schedule; results sit in @egress
#   until filter/flush returns them into the pipeline

def register(params)
  require "json"
  require "net/http"
  require "uri"

  @classify_url = (params["classify_url"] || "http://classify:8080").to_s.sub(%r{/+$}, "")
  @batch_url = "#{@classify_url}/classify/batch"
  @batch_size = (params["batch_size"] || 100).to_i
  @batch_size = 100 if @batch_size < 1
  @flush_ms = (params["flush_ms"] || 200).to_i
  @flush_ms = 200 if @flush_ms < 1

  @buffer = []
  @batch_started_at = nil
  @buffer_mutex = Mutex.new

  @uri = URI.parse(@batch_url)
  @http = Net::HTTP.new(@uri.host, @uri.port)
  @http.open_timeout = 5
  @http.read_timeout = 30
  @http.keep_alive_timeout = 60
  @http.start
  @http_mutex = Mutex.new

  @egress = []
  @egress_mutex = Mutex.new
  @stop = false
  @flusher = Thread.new { flush_loop }
end

def close
  @stop = true
  begin
    @flusher&.wakeup
  rescue ThreadError
    nil
  end
  @flusher&.join(2)

  # Best-effort: classify anything still buffered. Logstash should already have
  # called flush(final: true); this catches the rare close-without-final path.
  leftover = nil
  @buffer_mutex.synchronize do
    leftover = take_batch_locked! unless @buffer.empty?
  end
  if leftover
    finished = classify_events(leftover)
    @egress_mutex.synchronize { @egress.concat(finished) }
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

  batch = nil
  @buffer_mutex.synchronize do
    @buffer << event.clone
    @batch_started_at ||= monotonic_ms
    event.cancel
    batch = take_batch_locked! if @buffer.length >= @batch_size || buffer_aged?
  end

  out = []
  out.concat(classify_events(batch)) if batch
  out.concat(drain_egress)
  out
end

# Called by Logstash when periodic_flush => true, and with final=true on shutdown
def flush(options = {})
  final = false
  if options.respond_to?(:[])
    final = options[:final] || options["final"]
  end
  final = final == true || final.to_s == "true"

  batch = nil
  @buffer_mutex.synchronize do
    if final
      batch = take_batch_locked! unless @buffer.empty?
    elsif !@buffer.empty? && buffer_aged?
      batch = take_batch_locked!
    end
  end

  out = []
  out.concat(classify_events(batch)) if batch
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
    @egress_mutex.synchronize { @egress.concat(finished) }
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

# Caller must hold @buffer_mutex
def take_batch_locked!
  return nil if @buffer.empty?

  events = @buffer
  @buffer = []
  @batch_started_at = nil
  events
end

def drain_egress
  @egress_mutex.synchronize do
    return [] if @egress.empty?

    out = @egress
    @egress = []
    out
  end
end

def classify_events(events)
  return [] if events.nil? || events.empty?

  payloads = events.map do |e|
    {
      "sourcetype" => e.get("sourcetype").to_s,
      "source" => e.get("source").to_s,
      "message" => e.get("message").to_s,
      "splunk_index" => e.get("splunk_index").to_s
    }
  end

  results = post_batch(payloads)
  events.each_with_index do |e, i|
    apply_result(e, results[i])
  end
  events
end

def post_batch(payloads)
  req = Net::HTTP::Post.new(@uri.request_uri)
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
  @http = Net::HTTP.new(@uri.host, @uri.port)
  @http.open_timeout = 5
  @http.read_timeout = 30
  @http.keep_alive_timeout = 60
  @http.start
end

def fallback_result
  {
    "kind" => "generic",
    "dataset" => "generic",
    "namespace" => "default",
    "data_stream" => "logs-generic-default",
    "pipeline_name" => "frosty-parse-generic",
    "reason" => "fallback=batch_error"
  }
end

def apply_result(event, result)
  result = fallback_result if result.nil? || !result.is_a?(Hash)
  stream = result["data_stream"].to_s
  stream = "logs-generic-default" if stream.empty?
  kind = result["kind"].to_s
  kind = "generic" if kind.empty?
  dataset = result["dataset"].to_s
  dataset = "generic" if dataset.empty?
  namespace = result["namespace"].to_s
  namespace = "default" if namespace.empty?

  event.set("[@metadata][target_stream]", stream)
  event.set("[event][kind]", kind)
  event.set("[event][dataset]", dataset)
  event.set("[data_stream][type]", "logs")
  event.set("[data_stream][dataset]", dataset)
  event.set("[data_stream][namespace]", namespace)
  event.set("[splunk][pipeline]", result["pipeline_name"].to_s)
  event.set("[splunk][classify_reason]", result["reason"].to_s)
  event.set("[splunk][index]", event.get("splunk_index").to_s)
  if result["reason"].to_s.include?("batch_error")
    tags = event.get("tags") || []
    tags = [tags] unless tags.is_a?(Array)
    tags << "_classify_failed" unless tags.include?("_classify_failed")
    event.set("tags", tags)
  end
end
