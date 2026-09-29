with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view explode(discounts) e as tier_value
),
filtered as (
    -- Basic cleansing and defaults
    select *, coalesce(tier_value, 0) as tier_value_filled
    from lines
    where effective_from >= date '2024-09-01'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        max(tier_value_filled) over w_hist as plan_max_tier_value_filled,
        rank() over (order by tier_value desc) as tier_value_rank,
        rank() over (partition by plan order by tier_value_filled nulls first) as tier_value_filled_rank,
        dense_rank() over (order by tier_value desc nulls first) as tier_value_dense_rank,
        min(tier_value) over w_hist as plan_min_tier_value
    from filtered
    window w_hist as (partition by plan)
)
select
    id,
    plan,
    effective_from,
    tier_value,
    tier_value_filled,
    tier_value_rank,
    tier_value_filled_rank,
    tier_value_dense_rank,
    plan_min_tier_value,
    case when plan rlike '^(pro|starter)$' then 'EXTENDED' when plan rlike '^(basic|enterprise)$' then 'GROUP_A' else 'GROUP_B' end as plan_zone
from with_windows
order by tier_value_filled_rank desc nulls last;
