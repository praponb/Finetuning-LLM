WITH top_orders AS (
    SELECT
        id,
        store_id,
        amount,
        order_date,
        RANK() OVER (PARTITION BY store_id ORDER BY amount DESC) AS pos
    FROM orders
)
SELECT *
FROM top_orders
WHERE pos = 1
ORDER BY pos DESC
