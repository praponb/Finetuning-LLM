WITH orders_by_store AS (
    SELECT store_id, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count
    FROM orders
    GROUP BY store_id
)
SELECT s.id, s.store_name, obs.web_count
FROM stores s
LEFT OUTER JOIN orders_by_store obs ON s.id = obs.store_id
