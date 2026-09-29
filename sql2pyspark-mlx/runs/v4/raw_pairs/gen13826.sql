WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
)
SELECT
    k,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    COUNT(*) AS num_rows_2,
    COUNT(DISTINCT k) AS distinct_ks
FROM exploded
GROUP BY k;
