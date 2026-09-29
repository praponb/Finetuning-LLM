with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attributes,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code,
        array_contains(coupon_codes, 'WELCOME5') as used_welcome5,
        coalesce(attributes['utm_source'], 'none') as utm_source_or_default
    from carts
)
-- Running and moving aggregates
select
    id,
    coupon_codes,
    has_welcome_code,
    used_welcome5,
    utm_source_or_default,
    row_number() over (order by created_at desc, id) as seq_no
from flattened_attrs;
