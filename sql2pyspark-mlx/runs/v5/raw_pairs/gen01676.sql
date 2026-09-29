SELECT id, product_name, category, EXPLODE(tags) AS label FROM products;
