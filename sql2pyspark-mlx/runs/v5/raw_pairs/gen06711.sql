WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS rn
    FROM store_sales
)
SELECT
    id,
    store_id,
    sale_date,
    revenue
FROM ranked_store_sales
WHERE rn <= 3
ORDER BY sale_date, store_id DESC;
