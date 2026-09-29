select id, price, product_name, explode(tags) as tag_name from products where category = 'Fashion';
