select * from (select brand, round(avg(price), 2) as avg_price from products where array_contains(tags, 'bestseller') group by brand) as sub where avg_price >= 41;
