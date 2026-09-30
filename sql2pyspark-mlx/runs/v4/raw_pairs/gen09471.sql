SELECT id, brand, EXPLODE(tags) AS tag FROM products WHERE cost >= 240;
