with flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        items,
        ship_to,
        attributes,
        size(items) as num_items,
        aggregate(items, 0D, (acc, li) -> acc + li.qty * li.price, acc -> round(acc, 2)) as cart_value,
        transform(
            filter(items, li -> li.promo is not null),
            li -> struct(li.product_id as pid, round(li.price * 0.9, 2) as discounted_price)
        ) as promo_lines,
        array_contains(coupon_codes, 'VIP') as used_vip
    from carts
)
-- Multi-level aggregation
select
    customer_id,
    percentile_approx(num_items, 0.9) as num_items_p90,
    count(distinct id) as distinct_ids,
    min(num_items) as min_num_items,
    grouping(customer_id) as grouping_level
from flattened_attrs
group by rollup (customer_id)
having num_items_p90 >= 0
order by customer_id desc nulls first
