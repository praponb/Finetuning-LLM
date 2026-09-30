WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        level,
        status_code,
        context,
        context['region'] AS region,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
)
-- Running and moving aggregates
SELECT
    id,
    service,
    status_code,
    region,
    user_tier,
    FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY level, service ORDER BY status_code_high, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
    RANK() OVER (PARTITION BY service ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank,
    ROW_NUMBER() OVER (ORDER BY status_code DESC, id) AS seq_no
FROM flattened_attrs;
