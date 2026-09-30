select o.status, count(s.cost) cost_count, max(s.cost) max_cost
from shipments s
join orders o on s.order_id = o.id
group by o.status
order by cost_count, status
limit 5;
