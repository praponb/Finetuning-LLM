WITH parsed AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries AS retry_count,
        context['region'] AS region,
        context['version'] AS version
    FROM app_logs
)
SELECT
    *,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share_2
FROM parsed
