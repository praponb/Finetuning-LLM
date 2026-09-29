select signup_date, 'customer' origin
from customers
union
select order_date, 'order' origin
from orders
order by signup_date;
