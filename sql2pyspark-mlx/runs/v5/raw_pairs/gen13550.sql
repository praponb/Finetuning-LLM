with lines as (
    select
        id,
        plan,
        effective_from,
        explode(tier_limits) as val
    from price_tiers
),
valid as (
    select *, last_day(effective_from) as effective_from_month_end
    from lines
    where val <= 29
),
cleaned as (
    select *, substring(plan, 1, 3) as plan_prefix
    from valid
    where effective_from <= '2024-04-01'
),
with_windows as (
    select
        *,
        cume_dist() over (order by val asc nulls first) as val_cume_dist,
        rank() over (partition by plan order by val asc) as val_rank,
        avg(val) over (partition by plan) as plan_avg_val
    from cleaned
    where plan in ('pro', 'enterprise')
)
select
    plan,
    val,
    plan_prefix,
    val_cume_dist,
    val_rank
from with_windows
order by val desc, plan asc
