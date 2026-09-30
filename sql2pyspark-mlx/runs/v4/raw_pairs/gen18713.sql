SELECT id, price, EXPLODE(tags) AS label FROM products WHERE brand = 'Acme';
