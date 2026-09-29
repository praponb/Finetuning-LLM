WITH ProductSummary AS (
    SELECT product_id, COUNT(DISTINCT revenue) AS unique_products, COUNT(*) AS total_count
    FROM store_sales
    WHERE product_id = 10
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    P.unique_products,
    P.total_count
FROM products p
LEFT JOIN ProductSummary P ON p.id = P.product_id;
