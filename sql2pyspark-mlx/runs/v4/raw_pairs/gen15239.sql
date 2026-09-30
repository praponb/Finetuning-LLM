with cleaned as (
    select
        *,
        length(status) as status_length,
        coalesce(carrier, 'NONE') as resolved_carrier
    from shipments
    where delivered_date >= date '2024-01-26'
),
enriched as (
    select a.*, b.status as order_status
    from cleaned a
    left join orders b
        on a.order_id = b.id
),
enriched_shipments as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_length,
        resolved_carrier,
        order_status,
        lead(cost, 1) over (partition by carrier order by status_length asc, id asc) as next_cost,
        ntile(4) over (partition by order_status order by status_length desc, id asc) as status_length_bucket
    from enriched
),
with_windows as (
    select
        *,
        count(status_length) over w_grp as carrier_count_status_length,
        avg(cost) over (partition by order_status order by status_length_bucket asc, id asc rows between 5 preceding and 1 preceding) as cost_avg_5_prior,
        sum(status_length) over (partition by order_status order by delivered_date asc, id asc rows between unbounded preceding and current row) as cumulative_status_length,
        dense_rank() over (partition by carrier order by status_length_bucket desc nulls first) as status_length_bucket_dense_rank
    from enriched_shipments
    window w_grp as (partition by carrier)
),
deduped as (
    select *, row_number() over (partition by status order by status_length_bucket_dense_rank desc, id asc) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
)
select
    id,
    shipped_date,
    cost,
    status_length,
    next_cost,
    status_length_bucket,
    cost_avg_5_prior,
    cumulative_status_length,
    status_length_bucket_dense_rank
from deduped
order by cumulative_status_length asc, cost asc
