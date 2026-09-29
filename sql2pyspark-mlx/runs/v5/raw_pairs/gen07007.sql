with valid as (
    -- Filter the raw rows and derive helper columns
    select *, concat(brand, ' - ', category) as brand_category
    from products
    where supplier_id is not null
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.category,
        s.brand,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.brand_category,
        x.order_item_unit_price,
        x.quantity
    from valid s
    join lateral (
            select c.unit_price as order_item_unit_price, c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id = 26
        )
),
kept_keys as (
    -- Set intersection on keys
    select brand, category
    from not_flagged
    intersect all
    select brand, category
    from products
    where category = 'Electronics'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    join kept_keys k on s.brand = k.brand and s.category = k.category
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept_rows s
    where exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id in (16, 33)
        )
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(quantity) over (partition by brand) as brand_sum_quantity,
        avg(stock_qty) over (partition by brand, category) as brand_avg_stock_qty
    from eligible
    where array_contains(tags, 'premium')
),
eligible_products as (
    -- Semi-join via a correlated subquery
    select s.*
    from enriched s
    where exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.store_id in (5, 6)
        )
)
select
    category,
    tags,
    order_item_unit_price,
    quantity,
    brand_sum_quantity,
    brand_avg_stock_qty
from eligible_products;
