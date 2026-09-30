SELECT id, product_name, EXPLODE(tags) AS label FROM products WHERE brand IN ('Umbrella', 'Acme', 'Initech')
