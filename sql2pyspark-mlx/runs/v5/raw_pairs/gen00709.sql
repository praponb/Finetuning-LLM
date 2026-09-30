with lines as (
    select
        id,
        plan,
        effective_from,
        currency,
        element
    from price_tiers
    lateral view explode(tier_prices) e as element
)
select
    id,
    plan,
    currency,
    element,
    case when currency rlike '^(THB|EUR)$' then 'CORE' when currency rlike '^(USD)$' then 'GROUP_A' else 'PRIORITY' end as currency_zone
from lines
order by id;
