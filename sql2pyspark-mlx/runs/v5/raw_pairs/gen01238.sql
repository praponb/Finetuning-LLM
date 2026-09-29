with lines as (
    select
        id,
        plan,
        effective_from,
        element
    from price_tiers
    lateral view explode(tier_prices) e as element
),
base as (
    select *, upper(plan) plan_upper
    from lines
    where effective_from > date '2024-06-06'
      and effective_from >= '2023-04-01'
),
filtered as (
    select
        id,
        plan,
        effective_from,
        plan_upper,
        date_sub(effective_from, 66) effective_from_minus_66d
    from base
    where effective_from >= date '2024-04-21'
),
cleaned as (
    select *, datediff(current_date(), effective_from_minus_66d) days_since_effective_from_minus_66d
    from filtered
    where effective_from >= date '2024-05-30'
),
valid as (
    select
        id,
        plan,
        effective_from,
        effective_from_minus_66d,
        dayofweek(effective_from) effective_from_dayofweek
    from cleaned
    where effective_from >= to_date('2024-05-19')
      and days_since_effective_from_minus_66d != 31
)
select
    plan,
    approx_percentile(effective_from_dayofweek, 0.5) as effective_from_dayofweek_p50,
    size(collect_set(plan)) as num_plans,
    percentile_approx(effective_from_dayofweek, 0.9) as effective_from_dayofweek_p90
from valid
group by plan;
