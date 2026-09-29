WITH flattened_attrs AS (
    SELECT
        id,
        log_ts,
        level,
        context,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        COALESCE(context['trace'], 'unknown') AS trace_or_default,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM app_logs
)
SELECT
    *,
    SUM(status_code_high) OVER (PARTITION BY level ORDER BY status_code_high ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_high,
    FIRST_VALUE(flag_bit8, TRUE) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_flag_bit8,
    PERCENT_RANK() OVER (ORDER BY flag_bit8 DESC NULLS LAST) AS flag_bit8_percent_rank
FROM flattened_attrs;
