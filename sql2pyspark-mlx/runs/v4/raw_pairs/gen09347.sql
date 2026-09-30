SELECT
    p.launch_date,
    p.cost,
    t.last_sale_date,
    t.avg_revenue
FROM products p
LEFT JOIN (SELECT product_id, MAX(sale_date) AS last_sale_date, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_revenue > 2900;
