select
    case when cost >= 105 then 'high' when cost >= 30 then 'mid' else 'low' end as cost_bucket,
    carrier,
    max(shipped_date) as last_shipped_date,
    avg(cost) as avg_cost
from shipments
where cost <= 89 or carrier in ('Kerry', 'UPS')
group by 1, 2;
