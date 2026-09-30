SELECT
    p.id,
    p.cost,
    p.supplier_id,
    t.avg_revenue
FROM products p
INNER JOIN (SELECT product_id, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_revenue > 1600
ORDER BY p.supplier_id DESC
