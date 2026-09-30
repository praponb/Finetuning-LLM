select cost
from products
except
select amount
from orders
where year(order_date) >= 2023;
