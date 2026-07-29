# frozen_string_literal: true
# Shared golden harness (MRI). Run from repo:
#   ruby splash/logstash/plugins/logstash-input-s2s/test_decoder.rb
#
# Fixtures live in splash/testdata/s2s/ (same corpus as Python pytest).
# Protocol changes require updating those fixtures and both suites.

$LOAD_PATH.unshift File.expand_path("lib", __dir__)
require "json"
require "logstash/inputs/s2s/decoder"

module LogStash; end unless defined?(LogStash)

GOLDEN_ROOT = File.expand_path("../../../testdata/s2s", __dir__)
manifest = JSON.parse(File.read(File.join(GOLDEN_ROOT, "manifest.json")))

def assert!(cond, msg)
  raise msg unless cond
end

manifest["cases"].each do |tc|
  blob = File.binread(File.join(GOLDEN_ROOT, tc["bin"]))
  expect = tc["expect"]
  session = LogStash::S2s::Session.new
  events = session.feed(blob)
  assert!(events.size == expect["events"], "#{tc['id']}: events=#{events.size} want #{expect['events']}")
  if expect.key?("message")
    assert!(events[0]["message"] == expect["message"], "#{tc['id']}: bad message")
  end
  if expect.key?("splunk_index")
    assert!(events[0]["splunk_index"] == expect["splunk_index"], "#{tc['id']}: bad index")
  end
  if expect.key?("sourcetype")
    assert!(events[0]["sourcetype"] == expect["sourcetype"], "#{tc['id']}: bad sourcetype")
  end

  stats = session.stats
  %w[frames_ok handshake_seen frames_bad_magic frames_bad_kv frames_oversized capabilities_replied].each do |key|
    next unless expect.key?(key)

    got = stats.public_send(key)
    assert!(got == expect[key], "#{tc['id']}: #{key}=#{got} want #{expect[key]}")
  end
  {
    "frames_ok_min" => :frames_ok,
    "frames_bad_magic_min" => :frames_bad_magic,
    "frames_bad_kv_min" => :frames_bad_kv,
    "frames_oversized_min" => :frames_oversized
  }.each do |key, attr|
    next unless expect.key?(key)

    got = stats.public_send(attr)
    assert!(got >= expect[key], "#{tc['id']}: #{attr}=#{got} want >= #{expect[key]}")
  end
  puts "ok #{tc['id']} events=#{events.size} frames_ok=#{stats.frames_ok} bad_magic=#{stats.frames_bad_magic} bad_kv=#{stats.frames_bad_kv}"
end

puts "all golden cases passed"
