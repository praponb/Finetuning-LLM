select id, category, brand, explode(tags) as label from products;
