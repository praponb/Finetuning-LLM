with parsed as (
    select
        id,
        customer_id,
        coupon_codes,
        items,
        ship_to,
        attributes,
        size(filter(items, x -> x.qty >= 3)) as bulk_lines,
        size(attributes) as num_attributes,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        size(items) as num_items
    from carts
    where array_contains(coupon_codes, 'SPRING10')
)
select
    id,
    customer_id,
    num_attributes,
    num_items,
    last_value(num_attributes, true) over (partition by customer_id order by bulk_lines, id rows between unbounded preceding and current row) as last_known_num_attributes,
    ntile(10) over (partition by customer_id order by num_attributes, id) as num_attributes_bucket
from parsed;
