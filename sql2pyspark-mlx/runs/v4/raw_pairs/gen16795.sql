select
    order_id,
    sum(cost) as total_cost,
    count(*) as cnt,
    sum(sum(cost)) over (order by order_id) as cumulative_cost
from shipments
where delivered_date <= '2024-11-01'
group by order_id
order by 1 desc
limit 5
