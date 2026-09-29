with flattened_attrs as (
    select
        id,
        created_at,
        ship_to,
        forall(items, i -> i.qty <= 5) as all_small_qty,
        size(items) as num_items,
        aggregate(items, 0.0D, (acc, i) -> acc + i.qty * i.price, acc -> round(acc, 2)) as cart_value,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code,
        array_max(transform(items, i -> i.price)) as max_item_price
    from carts
)
select
    id,
    created_at,
    ship_to,
    all_small_qty,
    num_items,
    cart_value,
    has_welcome_code,
    max_item_price,
    dense_rank() over (order by cart_value nulls first) as cart_value_dense_rank,
    last_value(max_item_price, true) over w_recent as last_known_max_item_price,
    last_value(max_item_price, true) over (order by num_items, id rows between unbounded preceding and current row) as last_known_max_item_price_2,
    row_number() over w_grp as event_seq,
    percent_rank() over w_recent_2 as cart_value_percent_rank
from flattened_attrs
window w_recent as (order by created_at, id rows between unbounded preceding and current row), w_grp as (order by cart_value desc, id), w_recent_2 as (order by cart_value desc nulls first);
