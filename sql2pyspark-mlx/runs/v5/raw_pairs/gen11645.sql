with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_prices) e as val
),
metrics as (
    -- Ranking and gap features
    select
        *,
        max(val) over (partition by currency) as currency_max_val,
        rank() over (order by val nulls first) as val_rank,
        percent_rank() over (partition by currency order by val desc nulls last) as val_percent_rank,
        sum(val) over (partition by currency) as currency_sum_val
    from flattened
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        val,
        val_rank,
        currency_sum_val,
        percent_rank() over (partition by currency order by currency_sum_val desc nulls first) as currency_sum_val_percent_rank,
        dense_rank() over (partition by plan order by val desc) as val_dense_rank,
        round(val_rank / sum(val_rank) over (partition by currency), 4) as val_rank_share
    from metrics
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by currency_sum_val_percent_rank nulls first) as currency_sum_val_percent_rank_dense_rank,
        dense_rank() over w_hist as val_rank_share_dense_rank,
        percent_rank() over w_hist as val_rank_share_percent_rank,
        dense_rank() over (partition by plan order by currency_sum_val_percent_rank desc nulls first) as currency_sum_val_percent_rank_dense_rank_2
    from with_windows
    window w_hist as (partition by plan order by val_rank_share desc nulls first)
),
summary as (
    -- Grouped metrics
    select
        plan,
        count(case when plan = 'basic' then 1 end) as basic_count,
        count(distinct plan) as distinct_plans,
        count(*) as num_rows,
        size(collect_set(plan)) as num_plans
    from scored
    group by plan
)
select
    plan,
    distinct_plans,
    num_rows,
    num_plans,
    case when plan rlike '^(starter|basic)$' then 'GROUP_A' when plan rlike '^(pro|enterprise)$' then 'PRIORITY' else 'GROUP_B' end as plan_zone
from summary
order by plan_zone desc nulls first, num_plans desc
