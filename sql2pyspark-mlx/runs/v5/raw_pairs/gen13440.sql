select
    id,
    customer_id,
    created_at,
    items,
    ship_to,
    attributes,
    transform(
        filter(items, x -> x.promo is not null),
        x -> struct(x.product_id as pid, round(x.price * 0.9, 2) as discounted_price)
    ) as promo_lines,
    ship_to.zip as ship_zip,
    coalesce(attributes['utm_source'], 'none') as utm_source_or_default,
    array_join(array_sort(coupon_codes), ',') as coupons_text
from carts;
