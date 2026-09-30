with flattened_attrs as (
    select
        id,
        created_at,
        ship_to,
        size(items) as num_items,
        array_join(array_sort(coupon_codes), ',') as coupons_text,
        ship_to.city as ship_city,
        transform(
            filter(items, x -> x.promo is not null),
            x -> struct(x.product_id as pid, round(x.price * 0.9, 2) as discounted_price)
        ) as promo_lines,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code
    from carts
)
select
    *,
    first_value(num_items, true) over (partition by ship_city order by created_at, id rows between unbounded preceding and current row) as first_known_num_items,
    last_value(num_items, true) over (order by num_items, id rows between unbounded preceding and current row) as last_known_num_items,
    last_value(num_items, true) over (order by created_at, id rows between unbounded preceding and current row) as last_known_num_items_2,
    avg(num_items) over (order by created_at, id rows between 2 preceding and current row) as num_items_avg_2
from flattened_attrs;
