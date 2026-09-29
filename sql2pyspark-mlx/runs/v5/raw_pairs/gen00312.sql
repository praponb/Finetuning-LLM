with src as (
    select *
    from products
    where supplier_id = 8
),
agg as (
    select supplier_id, round(stddev(cost), 2) as stddev_cost, min(launch_date) as first_launch_date
    from src
    group by supplier_id
)
select supplier_id, first_launch_date
from agg
