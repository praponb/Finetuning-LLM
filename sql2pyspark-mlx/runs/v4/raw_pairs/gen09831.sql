with filtered_order_items as (
    select *
    from order_items
    where quantity <= 5
),
order_id_summary as (
    select
        order_id,
        product_id,
        min(quantity) min_quantity,
        avg(unit_price) avg_unit_price
    from filtered_order_items
    group by order_id, product_id
)
select order_id, product_id, min_quantity
from order_id_summary
where min_quantity >= 7.6
