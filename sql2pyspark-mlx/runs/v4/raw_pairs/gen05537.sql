SELECT id, status_code, SHIFTRIGHT(status_code, 2) AS status_code_high, COALESCE(context['trace'], 'unknown') AS trace_or_default FROM app_logs
