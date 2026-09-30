with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(tier_prices) li as element
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        effective_from,
        element,
        concat(plan, ' - ', currency) as plan_currency
    from flattened
    where effective_from >= date '2024-07-22'
      and currency = 'USD'
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        effective_from,
        element,
        plan_currency,
        percent_rank() over (partition by plan order by element desc nulls last) as element_percent_rank,
        dense_rank() over (order by element desc nulls first) as element_dense_rank,
        rank() over (partition by plan order by element desc) as element_rank
    from valid
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, lower(plan) as plan_lower
    from scored
    where element between 38 and 61
),
valid_price_tiers as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(element_rank, 0) as element_rank_filled,
        round(element_percent_rank * 0.9, 2) as element_percent_rank_adjusted
    from base
    where effective_from >= date '2024-05-29'
      and element_percent_rank >= 0.68
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(element_percent_rank_adjusted * 100, 2) as element_percent_rank_adjusted_scaled
    from valid_price_tiers
    where element_dense_rank between 4 and 7
),
cleaned_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        effective_from,
        element_percent_rank,
        element_dense_rank,
        element_rank,
        element_rank_filled,
        element_percent_rank_adjusted,
        element_percent_rank_adjusted_scaled,
        element_dense_rank - element_percent_rank_adjusted_scaled as element_dense_rank_minus_element_percent_rank_adjusted_scaled
    from cleaned
    where effective_from > to_date('2024-07-07')
      and plan in ('enterprise', 'pro')
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        plan,
        element_percent_rank_adjusted,
        element_percent_rank
    from cleaned_price_tiers
) src
unpivot include nulls (
    value for metric in (element_percent_rank_adjusted, element_percent_rank)
);
