select case when cost >= 80 then 'high' when cost >= 35 then 'mid' else 'low' end as cost_bucket, concat_ws(', ', sort_array(collect_set(carrier))) as carrier_values, sum(case when status = 'RETURNED' then cost else 0 end) as returned_cost
from shipments
where order_id = 35 and carrier not in ('UPS', 'DHL') and order_id is not null
group by case when cost >= 80 then 'high' when cost >= 35 then 'mid' else 'low' end
