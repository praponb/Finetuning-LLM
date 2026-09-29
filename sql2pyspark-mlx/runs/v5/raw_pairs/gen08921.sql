WITH orders_by_store AS (
    SELECT store_id, ROUND(AVG(amount), 2) avg_amount, SUM(amount) total_amount
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    obs.avg_amount,
    obs.total_amount
FROM stores s
INNER JOIN orders_by_store obs ON s.id = obs.store_id
WHERE obs.total_amount > 5900;
