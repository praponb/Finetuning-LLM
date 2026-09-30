select o.customer_id, s.delivered_date, s.shipped_date
from orders o
left join shipments s on s.order_id = o.id
where s.cost > 59;
