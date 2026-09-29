select id, price, brand, explode(tags) as label from products where brand = 'Globex';
