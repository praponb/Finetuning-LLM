select id, product_name, category, explode(tags) as label from products;
