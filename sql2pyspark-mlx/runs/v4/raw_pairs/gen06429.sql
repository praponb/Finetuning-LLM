select id, price, launch_date from products where price > (select max(price) from products);
