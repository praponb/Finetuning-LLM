with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by supplier_id order by launch_date desc, id) as latest_rank
        from products
    ) t
    where latest_rank = 1
),
joined as (
    -- Attach suppliers attributes
    select
        s.*,
        p2.rating,
        p2.country
    from deduped s
    left join suppliers p2
        on s.supplier_id = p2.id
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.unit_price,
        top1.order_item_quantity
    from joined s
    left join lateral (
            select c.unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    product_name,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    latest_rank,
    country
from with_last
order by price nulls last, country desc
