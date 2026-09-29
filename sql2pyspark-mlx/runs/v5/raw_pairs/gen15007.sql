select id, price, product_name
from products
where price > (select max(price) from products)
order by product_name;
