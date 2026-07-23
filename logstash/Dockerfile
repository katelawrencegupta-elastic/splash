FROM docker.elastic.co/logstash/logstash:8.15.0

# Cooked S2S is handled by the splash-s2s-decode service, not an in-process input.
USER root
RUN bin/logstash-plugin install --no-verify logstash-filter-http
USER logstash
