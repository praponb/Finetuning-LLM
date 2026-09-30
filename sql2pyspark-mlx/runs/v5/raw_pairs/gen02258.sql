WITH top_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS pos
    FROM orders
)
SELECT *
FROM top_orders
WHERE pos <= 2;
