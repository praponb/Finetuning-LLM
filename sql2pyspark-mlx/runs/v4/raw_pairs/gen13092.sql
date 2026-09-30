SELECT p.category, p.launch_date, t.total_count
FROM products p
LEFT JOIN (SELECT product_id, COUNT(*) AS total_count
    FROM store_sales
    WHERE store_id = 3
    GROUP BY product_id) t ON p.id = t.product_id;
