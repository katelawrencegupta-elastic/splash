FROM docker.elastic.co/logstash/logstash:8.15.0

USER root
COPY plugins/logstash-input-s2s/logstash-input-s2s-1.0.0.gem /tmp/logstash-input-s2s-1.0.0.gem
RUN bin/logstash-plugin install --no-verify /tmp/logstash-input-s2s-1.0.0.gem \
  && bin/logstash-plugin install --no-verify logstash-filter-http \
  && rm -f /tmp/logstash-input-s2s-1.0.0.gem
USER logstash
