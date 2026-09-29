with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(tier_prices) as element
    from price_tiers
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        element,
        upper(currency) as currency_upper
    from exploded
    where effective_from >= date '2024-01-26'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        element,
        weekofyear(effective_from) as effective_from_weekofyear
    from valid
    where effective_from > date '2024-02-15'
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        element,
        effective_from_weekofyear,
        case when plan in ('enterprise', 'starter') then 'Tier 1' else 'Other' end as plan_group
    from cleaned
    where effective_from > date '2024-03-07'
)
-- Running and moving aggregates
select
    *,
    dense_rank() over (partition by plan order by element desc) as element_dense_rank,
    percent_rank() over (partition by plan order by element) as element_percent_rank,
    dense_rank() over (order by element desc nulls last) as element_dense_rank_2
from filtered
where year(effective_from) >= 2025;
