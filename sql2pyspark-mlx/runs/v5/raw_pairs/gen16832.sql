SELECT id, host, level, context, GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text, context['trace'] AS trace FROM app_logs;
