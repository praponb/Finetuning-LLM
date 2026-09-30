WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE store_id IN (1, 3) AND units <= 26
),
summary AS (
    SELECT store_id, COUNT(DISTINCT units) AS unique_stores
    FROM recent_store_sales
    GROUP BY store_id
)
SELECT *
FROM summary
WHERE unique_stores > 9
ORDER BY store_id DESC;
