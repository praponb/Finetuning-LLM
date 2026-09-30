with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_limits) li as element
),
valid as (
    -- Keep recent, valid rows
    select *, length(plan) as plan_length
    from flattened
    where currency = 'USD'
),
base as (
    -- Keep recent, valid rows
    select *, case when currency in ('USD', 'THB') then 'Priority' else 'Other' end as currency_group
    from valid
    where plan_length = 72
),
base_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        currency,
        element,
        plan_length,
        currency_group,
        element * plan_length as element_times_plan_length
    from base
    where currency in ('THB', 'USD', 'EUR')
),
stats as (
    -- Grouped metrics
    select
        currency,
        case when element >= 74 then 'LARGE' else 'SMALL' end as element_tier,
        max(plan_length) filter (where currency = 'USD') as usd_max_plan_length,
        approx_percentile(element, 0.95) as element_p95,
        count(distinct currency) as distinct_currencies
    from base_price_tiers
    group by rollup (currency, case when element >= 74 then 'LARGE' else 'SMALL' end)
)
select
    element_tier,
    usd_max_plan_length,
    element_p95
from stats
order by usd_max_plan_length nulls first, element_tier;
