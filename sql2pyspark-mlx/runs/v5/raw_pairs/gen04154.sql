with lines as (
    select
        id,
        plan,
        effective_from,
        explode_outer(tier_limits) as val
    from price_tiers
),
filtered as (
    select *, cast(val as string) as val_num
    from lines
    where effective_from > '2023-11-25'
),
enriched as (
    select
        id,
        effective_from,
        val,
        sum(val) over w_running as plan_sum_val,
        max(val) over w_running as plan_max_val,
        rank() over (order by val desc nulls first) as val_rank
    from filtered
    where val = 78
    window w_running as (partition by plan)
),
with_windows as (
    select *, dense_rank() over w_ordered as plan_sum_val_dense_rank
    from enriched
    window w_ordered as (order by plan_sum_val nulls first)
),
base as (
    select *, greatest(plan_max_val, plan_sum_val) as greatest_plan_max_val_plan_sum_val
    from with_windows
    where plan_sum_val_dense_rank <> 6
)
select
    effective_from,
    val,
    plan_sum_val,
    plan_max_val,
    val_rank,
    plan_sum_val_dense_rank,
    greatest_plan_max_val_plan_sum_val
from base
order by effective_from desc, plan_max_val;
