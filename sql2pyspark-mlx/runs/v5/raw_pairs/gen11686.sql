with filtered as (
    select
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        upper(brand) as brand_upper,
        coalesce(brand, 'UNKNOWN') as brand_filled
    from products
    where launch_date >= date '2023-05-04'
      and year(launch_date) = 2025
),
with_latest as (
    select
        s.id,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags,
        s.brand_upper,
        s.brand_filled,
        x.quantity,
        x.last_restock
    from filtered s
    join lateral (
            select c.quantity, c.last_restock
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_last as (
    select
        s.*,
        top1.order_item_unit_price,
        top1.order_item_quantity
    from with_latest s
    join lateral (
            select c.unit_price as order_item_unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from inventory x
            where x.product_id = s.id
              and year(x.last_restock) >= 2025
        )
),
scored as (
    select
        id,
        brand,
        price,
        brand_upper,
        brand_filled,
        last_restock,
        sum(price) over (partition by category, brand order by price, id rows between unbounded preceding and current row) as running_price,
        corr(stock_qty, cost) over (partition by category, brand) as corr_stock_qty_cost,
        row_number() over (partition by brand, category order by cost, id) as row_seq
    from eligible
    where order_item_quantity <= 4
),
latest as (
    select *
    from scored
    qualify row_number() over (partition by brand order by last_restock desc nulls last, id) = 1
),
metrics as (
    select
        *,
        ntile(5) over (partition by brand order by running_price desc, id) as running_price_bucket,
        rank() over (order by row_seq) as row_seq_rank
    from latest
)
select
    brand,
    count(*) as event_count,
    round(sum(running_price) / nullif(count(case when brand = 'Globex' then 1 end), 0), 2) as running_price_per_globex
from metrics
group by brand;
