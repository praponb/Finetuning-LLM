WITH base AS (SELECT * FROM inventory WHERE quantity <= 147), agg AS (SELECT store_id, ROUND(AVG(quantity), 2) AS avg_quantity FROM base GROUP BY store_id) SELECT * FROM agg ORDER BY store_id DESC
