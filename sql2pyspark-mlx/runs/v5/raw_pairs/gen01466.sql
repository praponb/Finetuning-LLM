select case when cost >= 79 then 'high' when cost >= 36 then 'mid' else 'low' end as cost_bucket, status, max(cost) as max_cost
from shipments
where (carrier = 'Kerry' and delivered_date <= '2023-06-01') or carrier = 'Flash'
group by cost_bucket, status;
