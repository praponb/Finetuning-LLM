WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        line.product_id,
        line.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) t AS pos, line
)
-- Grouped metrics
SELECT
    product_id,
    MAX(created_at) AS ended_at,
    ROUND(AVG(pos), 2) AS avg_pos,
    MAX(created_at) AS last_seen
FROM exploded
GROUP BY product_id
ORDER BY avg_pos DESC NULLS LAST;
