Gem::Specification.new do |s|
  s.name            = "logstash-input-s2s"
  s.version         = "1.0.0"
  s.licenses        = ["Apache-2.0"]
  s.summary         = "Splunk cooked S2S TCP input for Logstash"
  s.description     = "Terminates cooked Splunk S2S, replies to capabilities, and emits decoded events."
  s.authors         = ["splash"]
  s.email           = ["devnull@example.com"]
  s.homepage        = "https://github.com/local/splash"
  s.require_paths   = ["lib"]
  s.files           = Dir["lib/**/*", "*.gemspec"]
  s.metadata = {
    "logstash_plugin" => "true",
    "logstash_group" => "input"
  }
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_development_dependency "logstash-devutils"
end
