SELECT id, category, product_name, EXPLODE(tags) AS label FROM products WHERE category <> 'Sports';
