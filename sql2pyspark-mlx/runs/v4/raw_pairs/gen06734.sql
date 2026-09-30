WITH unpacked AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        payload_json,
        SIZE(context) AS num_context,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        context['trace'] AS trace,
        ARRAY_CONTAINS(MAP_KEYS(context), 'region') AS has_region
    FROM app_logs
)
SELECT
    id,
    service,
    host,
    log_ts,
    payload_json,
    num_context,
    user_tier,
    trace,
    has_region,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY num_context NULLS LAST) AS num_context_percent_rank,
    SUM(num_context) OVER w_running AS service_sum_num_context,
    AVG(num_context) OVER (PARTITION BY service ORDER BY num_context, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS num_context_avg_5_prior
FROM unpacked
WINDOW w_running AS (PARTITION BY service);
