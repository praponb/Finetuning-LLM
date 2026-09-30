SELECT p.id, p.price, p.brand
FROM products p
WHERE MONTH(p.launch_date) IN (2, 3, 8) AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.units >= 13)
