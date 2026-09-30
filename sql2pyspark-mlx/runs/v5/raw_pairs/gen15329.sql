with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) li as element
)
select
    currency,
    element,
    case when currency rlike '^(THB|USD)$' then 'OTHER_ZONE' when currency rlike '^(EUR)$' then 'EXTENDED' else 'GROUP_B' end as currency_zone
from lines
order by currency_zone nulls last
