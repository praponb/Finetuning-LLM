with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(status) as status_lower,
        substring(carrier, 1, 3) as carrier_prefix
    from shipments
    where order_id is not null
)
-- Join a dimension and aggregate
select
    o.channel,
    src.carrier,
    count(*) as cnt,
    avg(src.cost) as avg_cost
from valid src
left join orders o on src.order_id = o.id
group by rollup (o.channel, src.carrier)
order by channel desc
