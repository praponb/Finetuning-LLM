select o.channel, count(oi.id) as order_item_count, avg(oi.unit_price) as avg_unit_price
from orders o
left join order_items oi on oi.order_id = o.id
where o.discount is not null
group by o.channel
order by order_item_count desc, channel
limit 3
