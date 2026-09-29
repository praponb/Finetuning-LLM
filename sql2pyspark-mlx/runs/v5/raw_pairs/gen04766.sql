select
    o.id,
    o.customer_id,
    o.order_date,
    o.amount
from orders o
where o.channel in ('partner', 'web', 'store') and o.customer_id in (select id from customers where email like '%@startup.io');
