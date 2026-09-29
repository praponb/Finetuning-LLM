with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(cost, 2) over (order by shipped_date, id) as next_cost,
        sum(cost) over w_running as running_cost,
        avg(cost) over w_hist as cost_avg_6,
        round(cost / sum(cost) over (partition by status), 4) as cost_share
    from shipments
    window w_running as (order by shipped_date, id rows between unbounded preceding and current row), w_hist as (partition by order_id, carrier order by shipped_date, id rows between 6 preceding and current row)
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        carrier,
        shipped_date,
        cost,
        next_cost,
        running_cost,
        cost_share,
        length(carrier) as carrier_length
    from with_windows
    where shipped_date > to_date('2023-11-22')
      and order_id is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, least(next_cost, carrier_length) as least_next_cost_carrier_length
    from valid
    where shipped_date >= date '2023-08-05'
      and carrier is not null
      and carrier_length <= 75
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(cost / sum(cost) over (partition by carrier), 4) as cost_share_2,
        cume_dist() over w_part as carrier_length_cume_dist
    from base
    window w_part as (partition by carrier order by carrier_length desc nulls last)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        last_value(least_next_cost_carrier_length) ignore nulls over (partition by carrier order by carrier_length_cume_dist, id rows between unbounded preceding and current row) as last_known_least_next_cost_carrier_length,
        row_number() over (partition by carrier order by least_next_cost_carrier_length, id) as event_seq,
        avg(cost_share) over (partition by carrier order by running_cost, id rows between 2 preceding and 1 preceding) as cost_share_avg_2_prior
    from enriched
)
select
    id,
    carrier,
    shipped_date,
    cost,
    cost_share,
    carrier_length,
    least_next_cost_carrier_length,
    cost_share_2
from scored
order by least_next_cost_carrier_length desc;
