with filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        round(amount * 1.15, 2) as amount_scaled,
        lower(method) as method_lower
    from payments
    where paid_at >= date '2023-05-13'
      and order_id is not null
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval '30' day then 1 else 0 end as is_new_session
    from filtered
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Multi-level aggregation
select
    case when island_id >= 8 then 'PREMIUM' else 'STANDARD' end as island_id_tier,
    min(paid_at) as started_at,
    sort_array(collect_set(status)) as status_list,
    count(*) as event_count
from islands
group by 1
order by island_id_tier nulls first
