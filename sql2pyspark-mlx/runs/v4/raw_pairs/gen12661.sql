WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
)
SELECT
    k,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE k = 'utm_source') AS utm_source_rows,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(DISTINCT id) AS distinct_ids
FROM exploded
GROUP BY 1
HAVING span_minutes < 0;
