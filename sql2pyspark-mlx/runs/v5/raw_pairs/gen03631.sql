select id, price from products where price = (select min(price) from products);
