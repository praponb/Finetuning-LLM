with filtered as (
    select *, quantity * unit_price as line_total
    from order_items
    where product_id is not null
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by order_id order by line_total, id) as latest_rank
        from filtered
    ) x
    where latest_rank = 1
),
joined as (
    select
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.latest_rank,
        b.order_date,
        b.shipping_fee
    from ranked s
    inner join orders b
        on s.order_id = b.id
),
latest as (
    select *
    from (
        select *, row_number() over (partition by order_id order by latest_rank desc, id) as row_num
        from joined
    ) x
    where row_num = 1
),
ranked_order_items as (
    select
        id,
        product_id,
        latest_rank,
        order_date
    from (
        select *, row_number() over (partition by product_id order by order_date desc nulls first, id) as rank_in_group
        from latest
    ) t
    where rank_in_group = 1
),
base as (
    select *, date_add(order_date, 3) as order_date_plus_3d
    from ranked_order_items
    where product_id in (14, 17)
)
select
    s.id,
    s.product_id,
    s.latest_rank,
    s.order_date_plus_3d,
    (select avg(x.quantity) from order_items x where x.product_id = s.product_id) as avg_order_item_quantity
from base s
order by product_id desc nulls first;
