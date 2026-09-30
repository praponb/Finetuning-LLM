WITH flattened_attrs AS (
    SELECT
        id,
        host,
        context,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration AS duration_sec,
        COALESCE(context['trace'], 'none') AS trace_or_default,
        FROM_JSON(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier AS tier,
        context['version'] AS version
    FROM app_logs
)
SELECT
    id,
    host,
    flag_bit2,
    LEAD(flag_bit2) OVER (PARTITION BY tier ORDER BY duration_sec, id) AS next_flag_bit2,
    LAG(flag_bit2) OVER (PARTITION BY tier ORDER BY duration_sec, id) AS prev_flag_bit2,
    LEAD(duration_sec) OVER (PARTITION BY tier ORDER BY duration_sec, id) AS next_duration_sec,
    LAG(duration_sec) OVER (PARTITION BY tier ORDER BY duration_sec, id) AS prev_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY tier ORDER BY duration_sec, id) AS seq_no,
    SUM(duration_sec) OVER (PARTITION BY host, tier ORDER BY flag_bit2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
FROM flattened_attrs;
