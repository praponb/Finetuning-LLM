with valid as (
    -- Basic cleansing and defaults
    select
        *,
        cast(price as int) as price_int,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where launch_date >= to_date('2023-07-05')
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by supplier_id order by launch_date desc nulls first, id) as rn
    from valid
    qualify rn = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        brand,
        launch_date,
        tags,
        supplier_id,
        sum(stock_qty) over (partition by category order by price, id rows between unbounded preceding and current row) as running_sum_stock_qty,
        min(stock_qty) over (partition by category order by price, id rows between unbounded preceding and current row) as running_min_stock_qty,
        max(stock_qty) over (partition by category order by price, id rows between unbounded preceding and current row) as running_max_stock_qty
    from ranked
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.order_item_quantity,
        latest.unit_price
    from scored s
    left join lateral (
            select c.quantity as order_item_quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
)
select
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.running_sum_stock_qty,
    s.running_min_stock_qty,
    (select max(x.cost) from products x where x.supplier_id = s.supplier_id) as max_product_cost
from with_latest s
order by max_product_cost nulls last, running_sum_stock_qty
