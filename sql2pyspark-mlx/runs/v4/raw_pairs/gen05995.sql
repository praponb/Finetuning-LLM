with unpacked as (
    -- Semi-structured parsing
    select
        id,
        created_at,
        coupon_codes,
        items,
        transform(
            filter(items, item -> item.promo is not null),
            item -> struct(item.product_id as pid, round(item.price * 0.9, 2) as discounted_price)
        ) as promo_lines,
        ship_to.country as ship_country,
        array_contains(map_keys(attributes), 'device') as has_device
    from carts
    where array_contains(coupon_codes, 'BUNDLE')
)
-- Grouped metrics
select
    ship_country,
    max(created_at) as ended_at,
    sort_array(collect_set(ship_country)) as ship_country_list,
    count(case when ship_country = 'SG' then 1 end) as sg_count,
    sort_array(collect_set(ship_country)) as ship_country_list_2
from unpacked
group by ship_country
having sg_count > 1;
