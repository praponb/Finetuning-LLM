select id, product_name, brand, explode(tags) as tag_name from products;
