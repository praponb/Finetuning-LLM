select id, product_name, explode(tags) as tag_name from products where array_contains(tags, 'eco');
