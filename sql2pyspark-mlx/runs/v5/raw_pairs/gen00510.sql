with lines as (
    select
        id,
        plan,
        currency,
        explode(tier_limits) as tier_value
    from price_tiers
),
filtered as (
    select *, round(tier_value * 0.9, 2) as tier_value_scaled
    from lines
    where currency = 'THB'
),
filtered_price_tiers as (
    select *, tier_value_scaled + tier_value as tier_value_scaled_plus_tier_value
    from filtered
    where plan = 'pro'
),
cleaned as (
    select
        id,
        plan,
        currency,
        tier_value,
        tier_value_scaled,
        round(tier_value_scaled_plus_tier_value * 1.1, 2) as tier_value_scaled_plus_tier_value_scaled,
        tier_value * tier_value_scaled as tier_value_times_tier_value_scaled
    from filtered_price_tiers
    where tier_value between 60 and 70
),
valid as (
    select
        id,
        currency,
        tier_value,
        tier_value_scaled,
        tier_value_scaled_plus_tier_value_scaled,
        tier_value_times_tier_value_scaled,
        round(tier_value_scaled_plus_tier_value_scaled, 0) as tier_value_scaled_plus_tier_value_scaled_rounded,
        tier_value_scaled + tier_value as tier_value_scaled_plus_tier_value
    from cleaned
    where tier_value_scaled_plus_tier_value_scaled between 21 and 65
)
select
    id,
    currency,
    tier_value,
    tier_value_scaled,
    tier_value_scaled_plus_tier_value_scaled,
    tier_value_times_tier_value_scaled,
    tier_value_scaled_plus_tier_value_scaled_rounded,
    tier_value_scaled_plus_tier_value
from valid
order by tier_value_scaled_plus_tier_value_scaled_rounded, tier_value_scaled_plus_tier_value
