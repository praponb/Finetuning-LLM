select id, host, latency_ms, payload_json, context['version'] as version, shiftright(status_code, 2) as status_code_high from app_logs
