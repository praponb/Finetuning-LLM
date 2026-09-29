select
    status,
    carrier,
    count(case when status = 'DELIVERED' then 1 end) as delivered_count,
    round(stddev(cost), 2) as stddev_cost
from shipments
group by status, carrier
