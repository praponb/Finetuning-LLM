WITH store_stats AS (
    SELECT store_id, SUM(shipping_fee) total_shipping_fee, MAX(amount) - MIN(amount) amount_range
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    ss.total_shipping_fee,
    ss.amount_range
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.amount_range > 47;
