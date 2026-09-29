with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        explode(coupon_codes) as tag
    from carts
)
select
    created_at,
    tag,
    case when tag rlike '^(VIP|WELCOME5)$' then 'GROUP_B' when tag rlike '^(SPRING10|BUNDLE)$' then 'EXTENDED' else 'PRIORITY' end as tag_zone
from lines
order by tag desc, created_at;
