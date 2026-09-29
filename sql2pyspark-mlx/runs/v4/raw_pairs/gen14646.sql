with base as (
    -- Basic cleansing and defaults
    select
        *,
        lower(carrier) as carrier_lower,
        case when carrier in ('DHL', 'Flash') then 'Tier 1' else 'Other' end as carrier_group
    from shipments
    where delivered_date > to_date('2023-06-17')
      and carrier is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_lower,
        carrier_group,
        first_value(cost, true) over (partition by order_id order by shipped_date, id rows between unbounded preceding and current row) as first_known_cost,
        lag(cost, 2) over (partition by status order by shipped_date, id) as prev_cost,
        sum(cost) over (partition by order_id, status) as order_id_sum_cost
    from base
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by status, order_id order by cost desc nulls first) as cost_dense_rank,
        row_number() over (partition by carrier order by first_known_cost, id) as row_seq,
        sum(prev_cost) over (partition by order_id order by cost, id rows between unbounded preceding and current row) as cumulative_prev_cost,
        sum(prev_cost) over (partition by carrier order by order_id_sum_cost, id rows between unbounded preceding and current row) as cumulative_prev_cost_2,
        sum(prev_cost) over (partition by status order by first_known_cost, id rows between unbounded preceding and current row) as cumulative_prev_cost_3
    from enriched
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        status,
        carrier_lower,
        carrier_group,
        first_known_cost,
        prev_cost,
        order_id_sum_cost,
        cost_dense_rank,
        row_seq,
        cumulative_prev_cost,
        cumulative_prev_cost_3,
        cast(cumulative_prev_cost as double) as cumulative_prev_cost_num,
        greatest(cumulative_prev_cost_2, first_known_cost) as greatest_cumulative_prev_cost_2_first_known_cost,
        coalesce(carrier, 'UNKNOWN') as resolved_carrier
    from metrics
    where shipped_date > date '2024-02-04'
)
-- Native SQL PIVOT
select *
from (
    select
        order_id,
        status,
        first_known_cost
    from valid
) src
pivot (
    avg(first_known_cost) as total
    for status in ('IN_TRANSIT', 'DELIVERED')
);
