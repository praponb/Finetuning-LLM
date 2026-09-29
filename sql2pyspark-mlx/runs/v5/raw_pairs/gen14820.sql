WITH flattened_attrs AS (
    SELECT
        id,
        service,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        GET_JSON_OBJECT(payload_json, '$.user.tier') AS user_tier,
        context['region'] AS region,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM app_logs
)
SELECT
    *,
    LAG(has_fault) OVER (PARTITION BY service ORDER BY has_fault, id) prev_has_fault,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY has_fault, id) seq_no
FROM flattened_attrs
