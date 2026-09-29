with lines as (
    select
        id,
        plan,
        effective_from,
        tier_value
    from price_tiers
    lateral view explode(tier_limits) e as tier_value
),
valid as (
    select *, initcap(trim(plan)) as plan_title
    from lines
    where year(effective_from) = 2023
),
base as (
    select
        id,
        plan,
        effective_from,
        tier_value,
        plan_title,
        floor(tier_value) as tier_value_whole
    from valid
    where effective_from > '2024-06-01'
),
scored as (
    select
        *,
        avg(tier_value_whole) over (partition by plan) as plan_avg_tier_value_whole,
        percent_rank() over (partition by plan order by tier_value_whole nulls first) as tier_value_whole_percent_rank
    from base
)
select
    id,
    plan,
    effective_from,
    tier_value,
    plan_title
from scored
order by plan desc, id desc;
