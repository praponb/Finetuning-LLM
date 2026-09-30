SELECT id, price, product_name, EXPLODE(tags) AS label FROM products WHERE brand NOT IN ('Stark', 'Umbrella');
