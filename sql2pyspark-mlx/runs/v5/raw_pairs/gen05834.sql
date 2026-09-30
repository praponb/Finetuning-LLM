SELECT p.brand, p.cost, ss.sale_date
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
