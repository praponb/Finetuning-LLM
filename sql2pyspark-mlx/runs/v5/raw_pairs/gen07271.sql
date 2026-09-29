with kept_keys as (
    select carrier
    from shipments
    except
    select carrier
    from shipments
    where month(delivered_date) in (1, 3, 5)
),
reconciled as (
    select s.*
    from shipments s
    inner join kept_keys k on s.carrier = k.carrier
),
valid as (
    select
        carrier,
        delivered_date,
        cost,
        substring(status, 1, 4) as status_prefix,
        coalesce(carrier, 'DIRECT') as carrier_filled
    from reconciled
    where carrier is not null
      and delivered_date < '2024-05-01'
),
valid_shipments as (
    select
        carrier,
        status_prefix,
        carrier_filled,
        dayofweek(delivered_date) as delivered_date_dayofweek,
        coalesce(carrier, 'unassigned') as carrier_filled_2
    from valid
    where year(delivered_date) = 2023
),
filtered as (
    select *, substring(carrier, 1, 3) as carrier_prefix
    from valid_shipments
    where carrier is not distinct from 'Kerry'
      and delivered_date_dayofweek < 43
)
select
    case when delivered_date_dayofweek >= 75 then 'HIGH' else 'LOW' end as delivered_date_dayofweek_tier,
    round(avg(delivered_date_dayofweek), 2) as avg_delivered_date_dayofweek,
    count(*) as row_count,
    approx_percentile(delivered_date_dayofweek, 0.5) as delivered_date_dayofweek_p50
from filtered
group by case when delivered_date_dayofweek >= 75 then 'HIGH' else 'LOW' end
having round(avg(delivered_date_dayofweek), 2) > 0
order by avg_delivered_date_dayofweek desc nulls last;
