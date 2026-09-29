SELECT id, category, EXPLODE(tags) AS label FROM products WHERE cost >= 310
