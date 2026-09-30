select case when cost >= 87 then 'high' when cost >= 33 then 'mid' else 'low' end cost_bucket, status, min(shipped_date) min_shipped_date
from shipments
where (delivered_date < '2023-11-05' and order_id = 36) or cost < 65
group by 1, 2;
