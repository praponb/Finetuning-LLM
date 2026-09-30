SELECT
    id,
    host,
    level,
    status_code,
    context,
    SIZE(context) AS num_context,
    context['region'] AS region
FROM app_logs
