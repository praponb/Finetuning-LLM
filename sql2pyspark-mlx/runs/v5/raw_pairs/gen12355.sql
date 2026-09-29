with lines as (
    select
        id,
        plan,
        effective_from,
        explode_outer(discounts) as val
    from price_tiers
),
enriched as (
    select
        *,
        max(val) over (partition by plan) as plan_max_val,
        dense_rank() over (partition by plan order by val desc nulls last) as val_dense_rank
    from lines
    where plan in ('pro', 'starter')
),
with_windows as (
    select
        *,
        percent_rank() over (order by val_dense_rank desc) as val_dense_rank_percent_rank,
        rank() over (order by val_dense_rank desc) as val_dense_rank_rank,
        rank() over (order by plan_max_val asc nulls first) as plan_max_val_rank,
        rank() over (partition by plan order by val desc) as val_rank,
        stddev_samp(plan_max_val) over (partition by plan) as plan_stddev_samp_plan_max_val
    from enriched
),
scored as (
    select *, round(val_dense_rank_rank / sum(val_dense_rank_rank) over (partition by plan), 4) as val_dense_rank_rank_share
    from with_windows
),
base as (
    select
        id,
        plan,
        effective_from,
        val,
        plan_max_val,
        val_dense_rank_percent_rank,
        val_dense_rank_rank,
        plan_max_val_rank,
        val_rank,
        plan_stddev_samp_plan_max_val,
        val_dense_rank_rank_share,
        round(plan_stddev_samp_plan_max_val, 2) as plan_stddev_samp_plan_max_val_rounded
    from scored
    where plan in ('starter', 'pro')
)
select
    id,
    plan,
    val,
    plan_max_val,
    val_dense_rank_percent_rank,
    val_dense_rank_rank,
    plan_max_val_rank,
    val_rank,
    plan_stddev_samp_plan_max_val,
    case when plan rlike '^(starter|pro)$' then 'OTHER_ZONE' when plan rlike '^(basic|enterprise)$' then 'GROUP_B' else 'CORE' end as plan_zone
from base
order by val_dense_rank_percent_rank desc nulls first
