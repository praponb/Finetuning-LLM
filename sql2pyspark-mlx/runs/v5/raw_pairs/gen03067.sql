WITH store_summary AS (
    SELECT store_id, AVG(quantity) AS avg_quantity, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE product_id IN (1, 5, 6)
    GROUP BY store_id
)
SELECT
    s.sqft,
    s.store_name,
    ss.avg_quantity,
    ss.stddev_quantity
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
