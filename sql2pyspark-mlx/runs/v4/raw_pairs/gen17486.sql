select id, product_name, brand, explode(tags) as tag from products
