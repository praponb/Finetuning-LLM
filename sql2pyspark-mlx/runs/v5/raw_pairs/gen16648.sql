with valid as (
    -- Basic cleansing and defaults
    select
        *,
        floor(cost) as cost_whole,
        coalesce(brand, 'DIRECT') as brand_clean
    from products
    where brand is not null
      and category = 'Grocery'
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.store_id in (1, 5, 6)
        )
),
eligible_products as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.store_id in (1, 2, 3, 5)
        )
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.quantity,
        top1.unit_price
    from eligible_products s
    left join lateral (
            select c.quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
)
-- Window metrics over several different partitions
select
    id,
    brand,
    cost,
    stock_qty,
    launch_date,
    tags,
    quantity,
    unit_price,
    lag(unit_price) over (partition by supplier_id order by stock_qty, id) as prev_unit_price,
    lag(price) over (partition by supplier_id order by stock_qty, id) as prev_price,
    row_number() over (partition by supplier_id order by stock_qty, id) as seq_no
from with_last
where product_name like '%Mak%'
