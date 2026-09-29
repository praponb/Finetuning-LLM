with flattened as (
    select
        id,
        plan,
        effective_from,
        explode_outer(discounts) as val
    from price_tiers
),
base as (
    select *, trim(plan) as clean_plan
    from flattened
    where plan in ('basic', 'pro', 'starter')
),
base_price_tiers as (
    select *, date_format(effective_from, 'yyyy-MM') as effective_from_label
    from base
    where effective_from >= date '2024-03-11'
      and effective_from >= '2025-05-01'
),
with_windows as (
    select
        *,
        cume_dist() over (partition by plan order by val nulls first) as val_cume_dist,
        cume_dist() over (partition by plan order by val nulls last) as val_cume_dist_2,
        dense_rank() over (partition by plan order by val nulls last) as val_dense_rank
    from base_price_tiers
)
select
    case when val_cume_dist >= 0.71 then 'LARGE' else 'SMALL' end as val_cume_dist_tier,
    plan,
    sort_array(collect_set(plan)) as plan_list,
    percentile_approx(val_dense_rank, array(0.25, 0.5, 0.75)) as val_dense_rank_quartiles,
    count(*) as event_count,
    count_if(plan = 'starter') as starter_events,
    grouping(case when val_cume_dist >= 0.71 then 'LARGE' else 'SMALL' end) as grouping_level
from with_windows
group by cube (case when val_cume_dist >= 0.71 then 'LARGE' else 'SMALL' end, plan);
