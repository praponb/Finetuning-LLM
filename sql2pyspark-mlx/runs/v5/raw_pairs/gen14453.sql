SELECT products.stock_qty, t.unique_stores
FROM products
LEFT JOIN (SELECT product_id, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    WHERE sale_date > '2024-03-19'
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.unique_stores >= 5
ORDER BY stock_qty;
