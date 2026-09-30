with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_limits) t as val
),
valid as (
    -- Basic cleansing and defaults
    select *, upper(currency) as currency_upper
    from exploded
    where effective_from > date '2024-04-25'
      and currency <> 'THB'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        effective_from,
        currency,
        val,
        currency_upper,
        substring(currency, 1, 2) as currency_prefix,
        trim(currency) as clean_currency
    from valid
    where val between 36 and 65
),
matched_keys as (
    -- Set difference on keys
    select currency
    from cleaned
    except
    select currency
    from price_tiers
    where plan not in ('basic', 'pro', 'enterprise')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.currency = k.currency
)
select
    currency,
    val,
    currency_upper,
    case when currency rlike '^(THB|EUR)$' then 'PRIORITY' when currency rlike '^(USD)$' then 'EXTENDED' else 'GROUP_A' end as currency_zone
from reconciled
order by currency desc;
