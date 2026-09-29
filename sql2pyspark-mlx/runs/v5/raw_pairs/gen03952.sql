select
    order_id,
    round(avg(cost), 2) as avg_cost,
    count(case when carrier = 'Kerry' then 1 end) as kerry_count,
    round(stddev(cost), 2) as stddev_cost
from shipments
group by order_id
having count(case when carrier = 'Kerry' then 1 end) > 2;
