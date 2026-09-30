with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) t as element
),
kept_keys as (
    -- Set intersection on keys
    select currency, id
    from exploded
    intersect all
    select currency, id
    from price_tiers
    where currency not in ('USD', 'EUR')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    join kept_keys k on s.currency = k.currency and s.id = k.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when element >= 64 then 'High' when element >= 20 then 'Medium' else 'Low' end as element_level,
        length(currency) as currency_length
    from verified
    where currency = 'EUR'
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        currency,
        element,
        currency_length,
        cast(currency_length as string) as currency_length_str,
        case when currency in ('THB', 'EUR') then 'Priority' else 'Other' end as currency_group
    from cleaned
    where currency_length != 58
),
cleaned_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(currency_length, 0) as currency_length_rounded,
        upper(currency) as currency_upper
    from base
    where currency_length < 72
)
-- Grouped metrics
select
    currency,
    case when currency_length >= 74 then 'HIGH' else 'LOW' end as currency_length_tier,
    count(case when currency = 'EUR' then 1 end) as eur_count,
    sum(currency_length_rounded) filter (where currency = 'EUR') as eur_total_currency_length_rounded,
    round(avg(currency_length), 2) as avg_currency_length,
    count(*) as row_count,
    count(*) filter (where currency = 'USD') as usd_rows
from cleaned_price_tiers
group by grouping sets ((currency, case when currency_length >= 74 then 'HIGH' else 'LOW' end), (currency))
