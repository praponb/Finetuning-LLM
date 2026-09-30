WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        context,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['region'], 'none') AS region_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier
    FROM app_logs
)
-- Ranking and gap features
SELECT
    *,
    SUM(flag_bit4) OVER (PARTITION BY tier) AS tier_sum_flag_bit4,
    ROW_NUMBER() OVER (PARTITION BY tier ORDER BY flag_bit4, id) AS event_seq,
    ROW_NUMBER() OVER w_recent AS row_seq
FROM flattened_attrs
WINDOW w_recent AS (PARTITION BY service ORDER BY flag_bit4, id)
