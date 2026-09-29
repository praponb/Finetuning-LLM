with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_prices) e as val
)
select
    id,
    currency,
    val,
    case when currency rlike '^(THB|EUR)$' then 'GROUP_A' else 'OTHER_ZONE' end as currency_zone
from flattened
