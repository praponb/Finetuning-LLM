select case when cost >= 101 then 'high' when cost >= 35 then 'mid' else 'low' end as cost_bucket, avg(cost) as avg_cost
from shipments
where status in ('LOST', 'RETURNED') or order_id in (9, 27, 30, 33)
group by case when cost >= 101 then 'high' when cost >= 35 then 'mid' else 'low' end
order by avg_cost, cost_bucket;
