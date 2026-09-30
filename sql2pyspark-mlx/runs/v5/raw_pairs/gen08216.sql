SELECT id, brand, product_name, EXPLODE(tags) AS label FROM products;
