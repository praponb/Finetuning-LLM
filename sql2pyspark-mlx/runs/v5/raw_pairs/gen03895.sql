with with_dims as (
    -- Attach orders attributes
    select e.*, o.channel
    from shipments e
    left join orders o
        on e.order_id = o.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        length(carrier) as carrier_length,
        coalesce(carrier, 'DIRECT') as carrier_clean
    from with_dims
    where channel is not null
      and carrier is distinct from 'UPS'
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify rank() over (partition by status order by cost desc nulls last) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        max(carrier_length) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) as running_max_carrier_length,
        sum(carrier_length) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) as running_sum_carrier_length,
        avg(carrier_length) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) as running_avg_carrier_length
    from ranked
    where year(delivered_date) >= 2024
),
remaining_keys as (
    -- Set intersection on keys
    select carrier
    from enriched
    intersect
    select carrier
    from shipments
    where cost >= 65
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.carrier = k.carrier
),
enriched_shipments as (
    -- Window metrics over several different partitions
    select
        id,
        carrier,
        shipped_date,
        status,
        running_max_carrier_length,
        running_sum_carrier_length,
        running_avg_carrier_length,
        percent_rank() over (partition by carrier order by carrier_length nulls first) as carrier_length_percent_rank
    from kept_rows
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        shipped_date,
        running_sum_carrier_length,
        running_avg_carrier_length,
        carrier_length_percent_rank,
        coalesce(carrier, 'N/A') as carrier_filled
    from enriched_shipments
    where shipped_date > date '2023-04-17'
      and carrier is not null
)
-- Running and moving aggregates
select
    shipped_date,
    running_sum_carrier_length,
    running_avg_carrier_length,
    carrier_length_percent_rank,
    rank() over (order by carrier_length_percent_rank) as carrier_length_percent_rank_rank,
    cume_dist() over (order by carrier_length_percent_rank desc nulls last) as carrier_length_percent_rank_cume_dist
from filtered;
