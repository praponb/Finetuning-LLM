select o.id, o.store_id
from orders o
where o.customer_id not in (select id from customers where country = 'TH');
