with unpacked as (
    select
        id,
        customer_id,
        ship_to,
        attributes,
        size(attributes) as num_attributes,
        transform(
            filter(items, item -> item.promo is not null),
            item -> struct(item.product_id as pid, round(item.price * 0.9, 2) as discounted_price)
        ) as promo_lines,
        attributes['gift'] as gift,
        transform(items, item -> item.product_id) as product_ids
    from carts
    where customer_id is null
)
select
    customer_id,
    approx_percentile(num_attributes, array(0.25, 0.5, 0.75)) as num_attributes_quartiles,
    count(distinct gift) as distinct_gifts,
    round(avg(num_attributes), 2) as avg_num_attributes,
    count(*) as row_count,
    avg(num_attributes) as avg_num_attributes_2,
    count(*) as cnt
from unpacked
group by rollup (customer_id)
having distinct_gifts < 1
order by row_count
