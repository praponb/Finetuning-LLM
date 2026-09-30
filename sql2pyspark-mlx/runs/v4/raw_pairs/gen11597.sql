select price
from products
except
select amount
from orders
where month(order_date) in (2, 5, 6)
order by price;
