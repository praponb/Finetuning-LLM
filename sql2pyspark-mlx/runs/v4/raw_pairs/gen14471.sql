with top_order_items as (
    select
        id,
        product_id,
        unit_price,
        quantity,
        row_number() over (partition by product_id order by unit_price desc, id) as rnk
    from order_items
)
select *
from top_order_items
where rnk = 1
