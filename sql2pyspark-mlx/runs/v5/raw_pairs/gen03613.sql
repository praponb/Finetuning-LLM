WITH product_summary AS (
    SELECT product_id, MAX(revenue) AS max_revenue
    FROM store_sales
    WHERE units != 40
    GROUP BY product_id
)
SELECT p.id, p.supplier_id, ps.max_revenue
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY max_revenue, id
LIMIT 20
