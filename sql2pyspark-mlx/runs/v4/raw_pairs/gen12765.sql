with with_dims as (
    -- Attach orders attributes
    select
        src.*,
        o.amount,
        o.channel,
        o.order_date
    from shipments src
    join orders o
        on src.order_id = o.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lead(cost, 1) over (order by delivered_date, id) next_cost,
        first_value(carrier, true) over w_recent first_known_carrier
    from with_dims
    window w_recent as (order by delivered_date, id rows between unbounded preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from with_windows
    qualify row_number() over (partition by status order by delivered_date desc, id) = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by carrier order by next_cost desc nulls first) as rank_in_group
    from deduped
    qualify rank_in_group = 1
),
with_dims_shipments as (
    -- Attach orders attributes
    select src.*, b.order_date as order_order_date
    from latest src
    left join orders b
        on src.order_id = b.id
),
with_windows_shipments as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by carrier order by next_cost, id) event_seq,
        sum(next_cost) over (partition by carrier, status order by order_order_date, id rows between unbounded preceding and current row) running_next_cost,
        first_value(next_cost) ignore nulls over (partition by channel order by shipped_date, id rows between unbounded preceding and current row) first_known_next_cost
    from with_dims_shipments
),
daily as (
    -- Aggregate with conditional metrics
    select
        date_trunc('month', shipped_date) as activity_month,
        approx_percentile(next_cost, array(0.25, 0.5, 0.75)) as next_cost_quartiles,
        round(avg(first_known_next_cost), 2) as avg_first_known_next_cost,
        count(*) filter (where status = 'DELIVERED') as delivered_rows,
        count(*) as row_count,
        sum(cost) filter (where status = 'LOST') as lost_total_cost
    from with_windows_shipments
    group by date_trunc('month', shipped_date)
)
select
    activity_month,
    next_cost_quartiles,
    delivered_rows,
    row_count,
    lost_total_cost
from daily
order by row_count desc nulls last, activity_month
