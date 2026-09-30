select id, product_name, brand, explode(tags) as label from products where brand != 'Acme';
