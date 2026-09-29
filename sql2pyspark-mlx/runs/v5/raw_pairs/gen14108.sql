select
    id,
    coupon_codes,
    attributes,
    transform(items, item -> item.product_id) as product_ids,
    ship_to.country as ship_country,
    coalesce(attributes['utm_source'], 'none') as utm_source_or_default
from carts
where size(coupon_codes) > 2
