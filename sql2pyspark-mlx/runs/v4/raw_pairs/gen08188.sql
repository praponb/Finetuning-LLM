WITH orders_by_store AS (
    SELECT store_id, MAX(amount) - MIN(amount) amount_range
    FROM orders
    GROUP BY store_id
)
SELECT s.id, s.region, obs.amount_range
FROM stores s
LEFT JOIN orders_by_store obs ON s.id = obs.store_id
WHERE obs.amount_range > 22;
