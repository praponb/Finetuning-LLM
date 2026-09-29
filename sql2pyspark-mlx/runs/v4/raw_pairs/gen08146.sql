select o.channel, round(avg(s.cost), 2) as avg_cost
from shipments s
join orders o on o.id = s.order_id
where o.shipping_fee < 33
group by o.channel
order by channel
