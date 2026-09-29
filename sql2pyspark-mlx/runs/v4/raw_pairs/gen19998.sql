WITH parsed AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        context,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        context['version'] AS version
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2025-10-05'
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY host ORDER BY log_ts, id) AS event_seq,
    LAST_VALUE(tier, TRUE) OVER (ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_tier,
    FIRST_VALUE(tier) IGNORE NULLS OVER (PARTITION BY tier ORDER BY log_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_tier
FROM parsed;
