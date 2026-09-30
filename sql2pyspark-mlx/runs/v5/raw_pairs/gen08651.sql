WITH ranked AS (
    SELECT
        id,
        product_id,
        sale_date,
        RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS rn
    FROM store_sales
)
SELECT id, product_id, sale_date
FROM ranked
WHERE rn = 1
ORDER BY product_id DESC
