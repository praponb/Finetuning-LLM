SELECT ss.id, ss.sale_date, ss.revenue
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE category = 'Grocery');
