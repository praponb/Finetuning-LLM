SELECT id, brand, cost * price AS margin FROM products WHERE LOWER(product_name) LIKE '%sb-%' AND stock_qty <= 356
