with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        substring(status, 1, 4) as status_prefix
    from payments
    where paid_at > timestamp '2023-10-07 14:00:00'
      and order_id is null
),
kept_keys as (
    -- Set intersection on keys
    select id, order_id
    from filtered
    intersect all
    select id, order_id
    from payments
    where amount >= 1220
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join kept_keys k on s.id = k.id and s.order_id = k.order_id
),
with_windows as (
    -- Running and moving aggregates
    select *, dense_rank() over w_part as amount_dense_rank
    from kept_rows
    where method = 'paypal'
    window w_part as (partition by order_id order by amount desc nulls last)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        status_prefix,
        amount_dense_rank,
        coalesce(amount_dense_rank, 0) as amount_dense_rank_filled,
        coalesce(method, 'DIRECT') as resolved_method
    from with_windows
    where paid_at > timestamp '2023-10-16 05:00:00'
      and order_id is not null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by method order by amount_dense_rank nulls first) as amount_dense_rank_dense_rank,
        dense_rank() over w_ordered as amount_dense_rank_dense_rank_2
    from base
    window w_ordered as (partition by method order by amount_dense_rank desc nulls first)
),
filtered_payments as (
    -- Basic cleansing and defaults
    select *, substring(method, 1, 2) as method_prefix
    from scored
    where method is not null
      and method is not distinct from 'promptpay'
),
stats as (
    -- Multi-level aggregation
    select
        case when amount_dense_rank >= 7 then 'LARGE' else 'SMALL' end as amount_dense_rank_tier,
        round(sum(amount_dense_rank_filled) / nullif(count(case when method = 'paypal' then 1 end), 0), 2) as amount_dense_rank_filled_per_paypal,
        approx_percentile(amount_dense_rank_dense_rank, array(0.25, 0.5, 0.75)) as amount_dense_rank_dense_rank_quartiles,
        count(distinct method) as distinct_methods
    from filtered_payments
    group by case when amount_dense_rank >= 7 then 'LARGE' else 'SMALL' end
)
select amount_dense_rank_tier
from stats
order by amount_dense_rank_tier desc nulls last
limit 20;
