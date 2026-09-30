SELECT id, service, host, level, status_code, context['version'] AS version, SIZE(context) AS num_context FROM app_logs WHERE service IN ('search', 'checkout');
