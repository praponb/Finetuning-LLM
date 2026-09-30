WITH product_summary AS (
    SELECT product_id, COUNT(*) AS num_store_sales
    FROM store_sales
    WHERE units <> 39
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.category,
    ps.num_store_sales
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.num_store_sales >= 9
ORDER BY p.brand, num_store_sales;
