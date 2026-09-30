SELECT id, service, log_ts, latency_ms, context, GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier, COALESCE(context['version'], 'none') AS version_or_default FROM app_logs
