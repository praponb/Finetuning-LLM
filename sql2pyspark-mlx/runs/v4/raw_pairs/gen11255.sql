WITH ranked_orders AS (
    SELECT *, RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) rnk
    FROM orders
)
SELECT
    id,
    channel,
    shipping_fee,
    customer_id
FROM ranked_orders
WHERE rnk <= 3
ORDER BY shipping_fee DESC;
