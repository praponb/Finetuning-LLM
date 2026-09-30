select signup_date
from customers
where is_active
intersect
select order_date
from orders
where discount >= 0.27
