select case when o.shipping_fee >= 39 then 'high' when o.shipping_fee >= 17 then 'mid' else 'low' end as shipping_fee_bucket, max(p.amount) - min(p.amount) as amount_range, min(p.amount) as min_amount
from payments p
inner join orders o on p.order_id = o.id
inner join stores s on o.store_id = s.id
group by case when o.shipping_fee >= 39 then 'high' when o.shipping_fee >= 17 then 'mid' else 'low' end;
