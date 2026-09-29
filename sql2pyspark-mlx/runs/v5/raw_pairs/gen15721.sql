with with_windows as (
    select
        *,
        ntile(10) over (order by cost desc, id) as cost_bucket,
        row_number() over (partition by carrier order by shipped_date desc, id) as row_seq,
        ntile(5) over (partition by status order by cost desc, id) as cost_bucket_2,
        rank() over (partition by order_id order by cost desc nulls last) as cost_rank,
        lead(cost, 1) over (partition by status order by cost, id) as next_cost
    from shipments
    where cost < 45
),
kept_keys as (
    select carrier, id
    from with_windows
    intersect
    select carrier, id
    from shipments
    where order_id is null
),
reconciled as (
    select s.*
    from with_windows s
    inner join kept_keys k on s.carrier = k.carrier and s.id = k.id
),
with_windows_shipments as (
    select
        *,
        rank() over (order by cost_bucket desc nulls first) as cost_bucket_rank,
        percent_rank() over (order by cost) as cost_percent_rank,
        rank() over (partition by order_id order by next_cost nulls last) as next_cost_rank,
        dense_rank() over (partition by order_id order by cost_bucket_2 nulls first) as cost_bucket_2_dense_rank,
        round(cost_bucket / sum(cost_bucket) over (partition by carrier), 4) as cost_bucket_share
    from reconciled
),
with_windows_shipments_2 as (
    select
        *,
        rank() over (order by cost_rank nulls first) as cost_rank_rank,
        percent_rank() over (partition by status order by cost_bucket_share desc) as cost_bucket_share_percent_rank,
        round(next_cost_rank / sum(next_cost_rank) over (partition by carrier), 4) as next_cost_rank_share
    from with_windows_shipments
)
select
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    cost_bucket,
    row_seq,
    cost_bucket_2
from with_windows_shipments_2
order by delivered_date nulls first;
