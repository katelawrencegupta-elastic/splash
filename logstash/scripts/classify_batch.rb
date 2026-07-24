# frozen_string_literal: true

# Buffer events and classify via POST /classify/batch.
# script_params: classify_url, batch_size, flush_ms, max_buffer, max_egress,
#                message_prefix_bytes
#
# - Reuses one Net::HTTP keep-alive connection
# - Takes batches under the buffer mutex; HTTP runs outside that lock
# - Bounded @buffer (filter workers block when full → TCP backpressure)
# - Bounded @egress (flusher waits to push; workers drain on each filter call)
# - Omits message when sourcetype/source present; otherwise sends a short prefix
# - A flusher thread wakes every flush_ms so idle buffers don't wait on
#   Logstash's coarser periodic_flush schedule; results sit in @egress
#   until filter/flush returns them into the pipeline

def register(params)
  require "json"
  require "net/http"
  require "thread"
  require "uri"

  @classify_url = (params["classify_url"] || "http://classify:8080").to_s.sub(%r{/+$}, "")
  @batch_url = "#{@classify_url}/classify/batch"
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

  @buffer = []
  @batch_started_at = nil
  @buffer_mutex = Mutex.new
  @buffer_cv = ConditionVariable.new

  @uri = URI.parse(@batch_url)
  @http = Net::HTTP.new(@uri.host, @uri.port)
  @http.open_timeout = 5
  @http.read_timeout = 30
  @http.keep_alive_timeout = 60
  @http.start
  @http_mutex = Mutex.new

  @egress = []
  @egress_mutex = Mutex.new
  @egress_cv = ConditionVariable.new
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
  @buffer_mutex.synchronize { @buffer_cv.broadcast }
  @egress_mutex.synchronize { @egress_cv.broadcast }
  @flusher&.join(2)

  # Best-effort: classify anything still buffered. Logstash should already have
  # called flush(final: true); this catches the rare close-without-final path.
  leftover = nil
  @buffer_mutex.synchronize do
    leftover = take_batch_locked! unless @buffer.empty?
  end
  if leftover
    finished = classify_events(leftover)
    push_egress(finished)
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
  accepted = false
  @buffer_mutex.synchronize do
    # Block only on @buffer depth. Flusher take_batch frees space and wakes us;
    # do not wait on @egress here (that would deadlock when workers cannot drain).
    while !@stop && @buffer.length >= @max_buffer
      @buffer_cv.wait(@buffer_mutex, 0.05)
    end

    unless @stop
      # Buffer the original event, then cancel it so it does not continue the
      # pipeline. Cancelled events must not be returned later — we clear the
      # flag when re-injecting (see rearm_event!).
      @buffer << event
      @batch_started_at ||= monotonic_ms
      event.cancel
      accepted = true
      batch = take_batch_locked! if @buffer.length >= @batch_size || buffer_aged?
    end
  end

  unless accepted
    # Shutting down with a full buffer: fail closed rather than unbounded growth.
    apply_result(event, fallback_result)
    return [event]
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

# Caller must hold @buffer_mutex
def take_batch_locked!
  return nil if @buffer.empty?

  events = @buffer
  @buffer = []
  @batch_started_at = nil
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

def classify_events(events)
  return [] if events.nil? || events.empty?

  payloads = events.map { |e| build_payload(e) }
  results = post_batch(payloads)
  events.each_with_index do |e, i|
    rearm_event!(e)
    apply_result(e, results[i])
  end
  events
end

# Skip full message when metadata can classify; otherwise send a short prefix.
def build_payload(event)
  sourcetype = event.get("sourcetype").to_s
  source = event.get("source").to_s
  payload = {
    "sourcetype" => sourcetype,
    "source" => source,
    "splunk_index" => event.get("splunk_index").to_s
  }
  if sourcetype.empty? && source.empty?
    message = event.get("message").to_s
    payload["message"] = if message.bytesize > @message_prefix_bytes
                            message.byteslice(0, @message_prefix_bytes)
                          else
                            message
                          end
  end
  payload
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
    "reason" => "fallback=batch_error",
    "fallback" => true
  }
end

# Events buffered then cancelled must be rearmed before re-injection.
def rearm_event!(event)
  return unless event.respond_to?(:cancelled?) && event.cancelled?

  if event.respond_to?(:uncancel)
    event.uncancel
  else
    # Logstash::Event stores cancellation in @cancelled (no public uncancel).
    event.instance_variable_set(:@cancelled, false)
  end
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

  is_fallback = result["fallback"] == true || result["fallback"].to_s == "true"
  if is_fallback
    tags = event.get("tags") || []
    tags = [tags] unless tags.is_a?(Array)
    tags << "_classify_failed" unless tags.include?("_classify_failed")
    event.set("tags", tags)
  end
end
