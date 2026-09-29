WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS rn
    FROM store_sales
)
SELECT id, product_id, units
FROM ranked_store_sales
WHERE rn <= 5
