with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) e as key, value
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        key,
        tag
    from lines
    lateral view outer explode(coupon_codes) x as tag
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, concat(tag, ' - ', key) as tag_key
    from flattened
    where tag is not null
)
select
    key,
    tag,
    case when tag rlike '^(WELCOME5|VIP)$' then 'EXTENDED' when tag rlike '^(SPRING10|BUNDLE)$' then 'PRIORITY' else 'GROUP_B' end as tag_zone
from valid
order by tag desc;
