WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
)
SELECT
    customer_id,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(*) AS cnt,
    SUM(product_id) AS total_product_id
FROM flattened
GROUP BY CUBE (customer_id, DATE_TRUNC('HOUR', created_at));
