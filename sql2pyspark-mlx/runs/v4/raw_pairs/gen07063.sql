with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode(tier_prices) as val
    from price_tiers
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(val) over w_part as plan_min_val,
        max(val) over w_part as plan_max_val
    from exploded
    window w_part as (partition by plan)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over w_hist as val_cume_dist,
        count(plan_max_val) over (partition by plan) as plan_count_plan_max_val,
        cume_dist() over w_grp as val_cume_dist_2
    from metrics
    window w_hist as (partition by plan order by val desc), w_grp as (order by val desc nulls first)
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when val >= 74 then 'LARGE' else 'SMALL' end as val_tier,
        avg(plan_count_plan_max_val) filter (where plan = 'enterprise') as enterprise_avg_plan_count_plan_max_val,
        round(sum(val_cume_dist_2) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as val_cume_dist_2_per_enterprise
    from scored
    group by case when val >= 74 then 'LARGE' else 'SMALL' end
)
select val_tier, enterprise_avg_plan_count_plan_max_val
from grouped
order by enterprise_avg_plan_count_plan_max_val, val_tier;
