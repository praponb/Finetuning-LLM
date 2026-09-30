with parsed as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        transform(
            filter(items, item -> item.promo is not null),
            item -> struct(item.product_id as pid, round(item.price * 0.9, 2) as discounted_price)
        ) as promo_lines,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code,
        attributes['utm_source'] as utm_source,
        coalesce(attributes['gift'], 'direct') as gift_or_default,
        coalesce(ship_to.city, 'UNKNOWN') as city_or_unknown
    from carts
)
-- Window metrics over several different partitions
select
    id,
    created_at,
    city_or_unknown,
    first_value(utm_source, true) over w_hist as first_known_utm_source,
    first_value(gift_or_default, true) over w_hist as first_known_gift_or_default,
    first_value(city_or_unknown) ignore nulls over w_hist as first_known_city_or_unknown
from parsed
window w_hist as (partition by customer_id order by created_at, id rows between unbounded preceding and current row)
