SELECT id, price, product_name, EXPLODE(tags) AS tag_name FROM products WHERE brand = 'Initech'
