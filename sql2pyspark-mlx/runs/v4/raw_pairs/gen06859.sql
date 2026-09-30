with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        round(stock_qty / cost, 2) as stock_qty_per_cost,
        initcap(trim(product_name)) as product_name_title,
        coalesce(brand, 'UNKNOWN') as brand_filled
    from products
    where launch_date >= date '2024-02-05'
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by brand, supplier_id order by stock_qty_per_cost desc, id) as dedup_rank
        from cleaned
    ) t
    where dedup_rank = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.unit_price < 380
        )
)
select
    id,
    product_name,
    category,
    price,
    stock_qty,
    supplier_id,
    product_name_title,
    brand_filled
from not_flagged
