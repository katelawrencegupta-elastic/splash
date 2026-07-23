# frozen_string_literal: true

# Buffer events and classify via POST /classify/batch.
# script_params: classify_url, batch_size, flush_ms
# Enable periodic_flush in logstash.conf so idle buffers flush without waiting
# for another event.

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
  @mutex = Mutex.new
end

def log_error(message)
  return unless defined?(@logger) && @logger

  @logger.error(message)
rescue StandardError
  nil
end

def filter(event)
  out = []
  @mutex.synchronize do
    @buffer << event.clone
    @batch_started_at ||= monotonic_ms
    event.cancel
    out = flush_locked! if @buffer.length >= @batch_size || buffer_aged?
  end
  out
end

# Called by Logstash when periodic_flush => true
def flush(_options = {})
  out = []
  @mutex.synchronize do
    out = flush_locked! if !@buffer.empty? && buffer_aged?
  end
  out
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

def flush_locked!
  return [] if @buffer.empty?

  events = @buffer
  @buffer = []
  @batch_started_at = nil

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
  uri = URI.parse(@batch_url)
  http = Net::HTTP.new(uri.host, uri.port)
  http.open_timeout = 5
  http.read_timeout = 30
  req = Net::HTTP::Post.new(uri.request_uri)
  req["Content-Type"] = "application/json"
  req.body = JSON.generate("events" => payloads)
  resp = http.request(req)
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
  Array.new(payloads.length) { fallback_result }
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
