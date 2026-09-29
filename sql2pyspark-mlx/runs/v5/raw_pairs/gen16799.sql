WITH exploded AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, value
)
SELECT
    customer_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows
FROM exploded
GROUP BY customer_id
ORDER BY distinct_ids ASC NULLS FIRST;
