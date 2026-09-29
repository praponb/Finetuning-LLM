select case when cost >= 103 then 'high' when cost >= 25 then 'mid' else 'low' end as cost_bucket, max(delivered_date) as latest_delivered_date
from shipments
where cost > 60 and (order_id in (14, 18, 35) or delivered_date > '2024-06-01')
group by 1
order by latest_delivered_date;
