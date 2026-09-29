select id, brand, price, explode(tags) as tag_name from products where supplier_id in (16, 33);
