WITH ranked_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) rn
    FROM orders
)
SELECT
    id,
    store_id,
    shipping_fee,
    customer_id
FROM ranked_orders
WHERE rn = 1;
