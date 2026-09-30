WITH top_store_sales AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC) AS rnk
    FROM store_sales
    WHERE units BETWEEN 14 AND 35
)
SELECT *
FROM top_store_sales
WHERE rnk = 1
ORDER BY 1 DESC
