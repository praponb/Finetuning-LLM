with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(effective_from) as effective_from_month_end
    from exploded
    where tier_value between 25 and 72
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(tier_value) over (partition by plan) as plan_max_tier_value,
        count(tier_value) over (partition by currency) as currency_count_tier_value
    from base
),
valid as (
    -- Basic cleansing and defaults
    select
        effective_from,
        currency,
        tier_value,
        effective_from_month_end,
        currency_count_tier_value,
        plan_max_tier_value * tier_value as plan_max_tier_value_times_tier_value
    from with_windows
    where tier_value > 23
),
with_windows_price_tiers as (
    -- Ranking and gap features
    select
        *,
        sum(currency_count_tier_value) over (partition by currency) as currency_sum_currency_count_tier_value,
        percent_rank() over (partition by currency order by tier_value desc nulls last) as tier_value_percent_rank
    from valid
),
report as (
    select
        currency,
        tier_value,
        plan_max_tier_value_times_tier_value,
        currency_sum_currency_count_tier_value,
        tier_value_percent_rank
    from with_windows_price_tiers
    order by tier_value desc
)
select
    currency,
    tier_value,
    plan_max_tier_value_times_tier_value,
    currency_sum_currency_count_tier_value,
    tier_value_percent_rank,
    case when currency rlike '^(USD|THB)$' then 'GROUP_B' else 'EXTENDED' end as currency_zone
from report
