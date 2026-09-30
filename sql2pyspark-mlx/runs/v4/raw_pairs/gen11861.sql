WITH store_summary AS (
    SELECT store_id, AVG(discount) AS avg_discount
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.region,
    ss.avg_discount
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.avg_discount >= 0.19;
