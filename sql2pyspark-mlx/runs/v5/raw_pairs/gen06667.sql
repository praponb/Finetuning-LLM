SELECT id, log_ts, latency_ms, COALESCE(context['region'], 'unknown') AS region_or_default, context['region'] AS region FROM app_logs;
