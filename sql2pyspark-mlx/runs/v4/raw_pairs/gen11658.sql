select id, product_name, explode(tags) as tag_name from products where cost is not null
