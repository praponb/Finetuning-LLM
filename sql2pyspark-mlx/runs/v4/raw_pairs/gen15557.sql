with exploded as (
    select
        id,
        currency,
        element
    from price_tiers
    lateral view explode(discounts) t as element
),
valid as (
    select *, round(element, 1) as element_rounded
    from exploded
    where currency = 'EUR'
),
base as (
    select *, greatest(element, element_rounded) as greatest_element_element_rounded
    from valid
    where element_rounded <= 65
),
remaining_keys as (
    select id
    from base
    except all
    select id
    from price_tiers
    where effective_from <= '2024-09-01'
),
kept_rows as (
    select s.*
    from base s
    join remaining_keys k on s.id = k.id
),
scored as (
    select
        id,
        currency,
        element,
        element_rounded,
        greatest_element_element_rounded,
        min(element) over w_recent as currency_min_element,
        rank() over (partition by currency order by greatest_element_element_rounded) as greatest_element_element_rounded_rank
    from kept_rows
    window w_recent as (partition by currency)
)
select
    id,
    currency,
    element,
    element_rounded,
    greatest_element_element_rounded,
    currency_min_element,
    greatest_element_element_rounded_rank,
    case when currency rlike '^(USD|EUR)$' then 'PRIORITY' when currency rlike '^(THB)$' then 'CORE' else 'GROUP_B' end as currency_zone
from scored
order by element desc
