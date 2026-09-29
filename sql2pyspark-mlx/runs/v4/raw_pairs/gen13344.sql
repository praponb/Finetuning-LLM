select o.id, o.customer_id, o.order_date
from orders o
where o.customer_id in (select id from customers where credit_limit >= 5700)
