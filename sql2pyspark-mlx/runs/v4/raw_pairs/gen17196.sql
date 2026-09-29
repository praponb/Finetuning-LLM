with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        upper(currency) currency_upper
    from lines
    where effective_from > date '2024-09-20'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        currency_upper,
        rank() over (order by tier_value nulls last) tier_value_rank,
        rank() over (order by tier_value nulls last) tier_value_rank_2
    from filtered
),
enriched_price_tiers as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        currency_upper,
        tier_value_rank,
        tier_value_rank_2,
        max(tier_value) over w_running currency_max_tier_value,
        dense_rank() over (order by tier_value desc nulls last) tier_value_dense_rank
    from enriched
    window w_running as (partition by currency)
),
filtered_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        currency_upper,
        tier_value_rank,
        tier_value_rank_2,
        currency_max_tier_value,
        tier_value_dense_rank,
        tier_value * currency_max_tier_value tier_value_times_currency_max_tier_value
    from enriched_price_tiers
    where currency = 'THB'
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        tier_value_rank,
        tier_value_rank_2,
        tier_value_dense_rank,
        tier_value_times_currency_max_tier_value,
        tier_value_dense_rank - tier_value_times_currency_max_tier_value tier_value_dense_rank_minus_tier_value_times_currency_max_tier_value
    from filtered_price_tiers
    where tier_value_times_currency_max_tier_value <= 39
),
report as (
    select
        id,
        plan,
        effective_from,
        tier_value_rank,
        tier_value_rank_2,
        tier_value_dense_rank,
        tier_value_times_currency_max_tier_value,
        case when plan rlike '^(basic|enterprise)$' then 'GROUP_B' when plan rlike '^(starter|pro)$' then 'GROUP_A' else 'EXTENDED' end as plan_zone
    from valid
)
select
    effective_from,
    tier_value_rank_2,
    tier_value_times_currency_max_tier_value,
    plan_zone,
    case when plan_zone rlike '^(GROUP_A|GROUP_B)$' then 'GROUP_B' when plan_zone rlike '^(EXTENDED)$' then 'GROUP_A' else 'EXTENDED' end as plan_zone_zone
from report
order by plan_zone_zone, effective_from desc
