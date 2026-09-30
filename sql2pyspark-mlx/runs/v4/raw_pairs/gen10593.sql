with recent_shipments as (
    select *
    from shipments
    where delivered_date >= '2023-07-01' and shipped_date <= '2023-10-01'
),
shipments_stats as (
    select carrier, round(avg(cost), 2) as avg_cost, sum(cost) as total_cost
    from recent_shipments
    group by carrier
)
select carrier, avg_cost, total_cost
from shipments_stats
order by avg_cost desc, carrier asc
