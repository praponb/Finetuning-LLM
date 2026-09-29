select s.cost, s.delivered_date, o.status
from shipments s
join orders o on s.order_id = o.id
where o.discount >= 0.14
