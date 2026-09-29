select
    case when o.shipping_fee >= 43 then 'high' when o.shipping_fee >= 7 then 'mid' else 'low' end as shipping_fee_bucket,
    case when o.amount >= 1630 then 'high' when o.amount >= 780 then 'mid' else 'low' end as amount_bucket,
    sort_array(collect_set(s.carrier)) as carrier_list,
    count(s.cost) as cost_count
from shipments s
inner join orders o on o.id = s.order_id
group by case when o.shipping_fee >= 43 then 'high' when o.shipping_fee >= 7 then 'mid' else 'low' end, case when o.amount >= 1630 then 'high' when o.amount >= 780 then 'mid' else 'low' end
order by amount_bucket
