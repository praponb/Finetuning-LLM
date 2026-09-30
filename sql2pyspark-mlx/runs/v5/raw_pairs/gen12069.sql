with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
valid as (
    -- Keep recent, valid rows
    select *, length(currency) as currency_length
    from lines
    where tier_value > 76
),
with_windows as (
    -- Running and moving aggregates
    select *, max(currency_length) over w_ordered as currency_max_currency_length
    from valid
    where currency in ('THB', 'USD')
    window w_ordered as (partition by currency)
),
valid_price_tiers as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        currency,
        tier_value,
        currency_length,
        currency_max_currency_length,
        currency_length * tier_value as currency_length_times_tier_value,
        substring(currency, 1, 2) as currency_prefix
    from with_windows
    where currency = 'USD'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        currency,
        currency_length,
        currency_max_currency_length,
        currency_prefix,
        length(currency) as currency_length_2
    from valid_price_tiers
    where currency_max_currency_length = 66
),
cleaned_price_tiers as (
    -- Keep recent, valid rows
    select *, currency_length_2 - currency_length as currency_length_2_minus_currency_length
    from cleaned
    where currency = 'EUR'
)
-- Ranking and gap features
select
    currency,
    currency_max_currency_length,
    currency_length_2,
    currency_length_2_minus_currency_length,
    max(currency_length_2) over w_recent as currency_max_currency_length_2,
    stddev_samp(currency_length) over w_recent as currency_stddev_samp_currency_length,
    count(currency_max_currency_length) over w_recent as currency_count_currency_max_currency_length,
    sum(currency_length_2_minus_currency_length) over w_recent as currency_sum_currency_length_2_minus_currency_length
from cleaned_price_tiers
window w_recent as (partition by currency)
