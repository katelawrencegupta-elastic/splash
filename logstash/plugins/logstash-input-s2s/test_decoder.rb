# frozen_string_literal: true
# Quick decoder self-test (MRI). Run: ruby logstash/plugins/logstash-input-s2s/test_decoder.rb

$LOAD_PATH.unshift File.expand_path("lib", __dir__)
require "logstash/inputs/s2s/decoder"

module LogStash; end unless defined?(LogStash)

def enc_str(s)
  data = s.encode("UTF-8")
  [data.bytesize + 1].pack("N") + data.b + "\x00".b
end

def enc_kv(k, v)
  enc_str(k) + enc_str(v)
end

def make_sig(version: 3)
  banner = version >= 3 ? "--splunk-cooked-mode-v3--" : "--splunk-cooked-mode-v2--"
  buf = ("\x00" * 400).b
  buf[0, banner.bytesize] = banner.b
  buf[128, 5] = "host1"
  buf[384, 4] = "8089"
  buf
end

def make_event
  kvs = [
    ["_MetaData:Index", "apache"],
    ["MetaData:Host", "host::web1"],
    ["MetaData:Source", "source::/var/log/nginx/access.log"],
    ["MetaData:Sourcetype", "sourcetype::access_combined"],
    ["_time", "1721577600"],
    ["_done", "_done"],
    ["_raw", "hello-raw"]
  ]
  body = [kvs.length].pack("N")
  kvs.each { |k, v| body << enc_kv(k, v) }
  body << [0].pack("N")
  body << enc_str("_raw")
  [body.bytesize].pack("N") + body
end

def make_caps(caps = "ack=0;compression=0")
  kv = enc_kv("__s2s_capabilities", caps)
  body = [1].pack("N") + kv + [0].pack("N") + enc_str("_raw")
  [body.bytesize].pack("N") + body
end

session = LogStash::S2s::Session.new
blob = make_sig + make_caps + make_event
events = session.feed(blob)
raise "expected 1 event, got #{events.size}" unless events.size == 1
raise "bad message" unless events[0]["message"] == "hello-raw"
raise "bad index" unless events[0]["splunk_index"] == "apache"
raise "expected caps reply" unless session.stats.capabilities_replied == 1
replies = session.take_replies!
raise "no reply bytes" if replies.empty?
raise "reply missing control" unless replies[0].include?("__s2s_control_msg".b)
puts "ok events=#{events.size} caps=#{session.stats.capabilities_replied} frames=#{session.stats.frames_ok}"
