select id, order_date
from orders
where customer_id = 20
union all
select id, order_date
from orders
where discount between 0.23 and 0.33;
