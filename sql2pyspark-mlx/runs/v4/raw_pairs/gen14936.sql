SELECT id, product_name, brand, EXPLODE(tags) AS label FROM products
