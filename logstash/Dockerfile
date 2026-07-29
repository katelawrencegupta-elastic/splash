# logstash:8.15.0
FROM docker.elastic.co/logstash/logstash:8.15.0@sha256:73a30fb57f305c0a6e0018ef37aeda016b5a3578a95530af13579382938cc3d6

# Cooked S2S is handled by the splash-s2s-decode service, not an in-process input.
USER root
RUN bin/logstash-plugin install --no-verify logstash-filter-http
USER logstash
