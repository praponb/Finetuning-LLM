with base as (
    select *
    from shipments
    where delivered_date <= '2025-01-25'
),
order_id_summary as (
    select order_id, avg(cost) as avg_cost, round(stddev(cost), 2) as stddev_cost
    from base
    group by order_id
)
select order_id, avg_cost, stddev_cost
from order_id_summary
