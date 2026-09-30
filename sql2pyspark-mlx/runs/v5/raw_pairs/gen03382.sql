with latest as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        product_id,
        quantity
    from (
        select *, rank() over (partition by product_id, order_id order by unit_price desc nulls last) as row_num
        from order_items
    ) ranked
    where row_num = 1
)
-- Aggregate with conditional metrics
select
    product_id,
    order_id,
    bit_and(quantity) as quantity_bit_and,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    count(distinct order_id) as distinct_order_ids
from latest
group by cube (product_id, order_id)
order by product_id desc nulls first;
