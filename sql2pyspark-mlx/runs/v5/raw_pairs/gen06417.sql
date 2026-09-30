WITH product_summary AS (
    SELECT product_id, MAX(sale_date) AS last_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT p.product_name, p.category, ps.last_sale_date
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY last_sale_date DESC;
