WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rn
    FROM store_sales
)
SELECT id, store_id, revenue
FROM ranked_store_sales
WHERE rn <= 3
ORDER BY store_id DESC, revenue DESC
