select s.id, s.status, o.customer_id
from shipments s
right outer join orders o on s.order_id = o.id
where s.shipped_date > '2023-07-01';
