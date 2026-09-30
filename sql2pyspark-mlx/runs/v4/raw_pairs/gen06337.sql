select id, price, product_name, explode(tags) as tag from products
