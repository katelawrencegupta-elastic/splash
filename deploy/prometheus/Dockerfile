# prom/prometheus:v2.54.1
FROM prom/prometheus:v2.54.1@sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94

USER root
COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY prometheus.yml /etc/prometheus/prometheus.yml.tmpl
RUN chmod +x /docker-entrypoint.sh \
  && mkdir -p /etc/prometheus/secrets /etc/prometheus/rules \
  && chown -R nobody:nobody /etc/prometheus/secrets /tmp \
  && chmod 1777 /tmp

USER nobody
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--storage.tsdb.path=/prometheus", "--web.enable-lifecycle"]
