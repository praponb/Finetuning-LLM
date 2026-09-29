WITH src AS (
    SELECT *
    FROM order_items
    WHERE quantity != 5
),
summary AS (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(quantity) AS quantity_count, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM src
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END
)
SELECT quantity_bucket, quantity_count, avg_quantity
FROM summary
WHERE quantity_count >= 10
ORDER BY avg_quantity DESC, quantity_count DESC
