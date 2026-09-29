with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        tier_value
    from price_tiers
    lateral view explode(tier_prices) t as tier_value
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, month(effective_from) effective_from_month
    from exploded
    where currency in ('USD', 'EUR')
),
valid_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select *, weekofyear(effective_from) effective_from_weekofyear
    from valid
    where effective_from_month is not null
),
valid_price_tiers_2 as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        effective_from,
        tier_value,
        effective_from_month,
        effective_from_month * tier_value effective_from_month_times_tier_value
    from valid_price_tiers
    where month(effective_from) in (3, 4, 11)
),
kept_keys as (
    -- Set difference on keys
    select id
    from valid_price_tiers_2
    except distinct
    select id
    from price_tiers
    where month(effective_from) in (3, 5, 6)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid_price_tiers_2 s
    join kept_keys k on s.id = k.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, tier_value - effective_from_month_times_tier_value tier_value_minus_effective_from_month_times_tier_value
    from verified
    where tier_value < 80
),
report as (
    select
        id,
        effective_from,
        tier_value,
        effective_from_month,
        effective_from_month_times_tier_value,
        tier_value_minus_effective_from_month_times_tier_value
    from filtered
)
select
    id,
    effective_from,
    tier_value,
    effective_from_month,
    effective_from_month_times_tier_value,
    tier_value_minus_effective_from_month_times_tier_value
from report
order by effective_from_month;
