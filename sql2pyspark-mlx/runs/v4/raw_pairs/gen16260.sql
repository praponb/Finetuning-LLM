with src as (
    select *
    from shipments
    where year(shipped_date) = 2023
),
agg as (
    select status, round(stddev(cost), 2) as stddev_cost, count(distinct cost) as unique_carriers
    from src
    group by status
)
select status, stddev_cost, unique_carriers
from agg
where unique_carriers < 10
order by unique_carriers desc, status desc;
