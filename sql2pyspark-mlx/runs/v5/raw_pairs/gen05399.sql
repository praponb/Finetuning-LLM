select id, product_name, brand, tag from products lateral view explode(tags) t as tag
