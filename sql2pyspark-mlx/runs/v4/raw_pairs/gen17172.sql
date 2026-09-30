select case when o.amount >= 1320 then 'high' when o.amount >= 610 then 'mid' else 'low' end as amount_bucket, o.channel, max(oi.quantity) as max_quantity
from order_items oi
inner join products p on oi.product_id = p.id
inner join orders o on oi.order_id = o.id
group by case when o.amount >= 1320 then 'high' when o.amount >= 610 then 'mid' else 'low' end, o.channel
order by amount_bucket desc, max_quantity desc
