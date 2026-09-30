WITH top_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rnk
    FROM store_sales
    WHERE product_id IN (1, 5, 7, 9)
)
SELECT id, store_id, revenue
FROM top_store_sales
WHERE rnk = 1;
