with parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        created_at,
        items,
        ship_to,
        attributes,
        coalesce(attributes['device'], 'none') as device_or_default,
        exists(items, item -> item.price > 100) as has_premium_item,
        array_contains(coupon_codes, 'SPRING10') as used_spring10
    from carts
    where created_at >= '2024-08-01'
)
-- Window metrics over several different partitions
select
    id,
    created_at,
    device_or_default,
    has_premium_item,
    used_spring10,
    first_value(device_or_default) ignore nulls over (order by created_at asc, id asc rows between unbounded preceding and current row) as first_known_device_or_default,
    row_number() over w_recent as event_seq
from parsed
where not has_premium_item
window w_recent as (order by created_at asc, id asc)
