WITH ranked AS (
    SELECT
        id,
        store_id,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS rn
    FROM store_sales
    WHERE product_id IN (1, 2, 4, 5)
)
SELECT id, store_id, sale_date
FROM ranked
WHERE rn <= 2;
