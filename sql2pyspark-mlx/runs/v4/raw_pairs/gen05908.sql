select o.id, count(s.id) as shipment_count, round(stddev(s.cost), 2) as stddev_cost
from orders o
left outer join shipments s on s.order_id = o.id
group by o.id;
