with with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.unit_price,
        latest.quantity
    from products s
    join lateral (
            select c.unit_price, c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date < s.launch_date
        )
),
scored as (
    -- Ranking and gap features
    select
        *,
        lead(quantity) over (partition by brand order by price, id) as next_quantity,
        lag(quantity) over (partition by brand order by price, id) as prev_quantity,
        lead(price) over (partition by brand order by price, id) as next_price,
        lag(price) over (partition by brand order by price, id) as prev_price,
        row_number() over (partition by brand order by price, id) as row_seq
    from kept
    where quantity != 3
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        greatest(prev_quantity, next_price) as greatest_prev_quantity_next_price,
        coalesce(product_name, 'unassigned') as product_name_filled
    from scored
    where launch_date > date '2023-06-09'
      and brand is not null
),
base as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        unit_price,
        quantity,
        next_quantity,
        prev_quantity,
        next_price,
        row_seq,
        product_name_filled,
        round(greatest_prev_quantity_next_price, 1) as greatest_prev_quantity_next_price_rounded
    from valid
    where supplier_id is null
      and brand is not null
),
scored_products as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (partition by supplier_id order by greatest_prev_quantity_next_price_rounded desc) as greatest_prev_quantity_next_price_rounded_cume_dist,
        rank() over (partition by supplier_id order by quantity desc) as quantity_rank,
        lag(next_price, 2) over (order by next_quantity, id) as prev_next_price
    from base
),
valid_products as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(prev_next_price * 100, 2) as prev_next_price_scaled,
        coalesce(brand, 'NONE') as brand_filled
    from scored_products
    where brand is distinct from 'Acme'
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        supplier_id,
        next_price,
        greatest_prev_quantity_next_price_rounded_cume_dist,
        prev_next_price
    from valid_products
) src
unpivot (
    val for measure in (next_price, greatest_prev_quantity_next_price_rounded_cume_dist as GREATEST_PREV_QUANTITY_NEXT_PRICE_ROUNDED_CUME_DIST_VAL, prev_next_price as PREV_NEXT_PRICE)
)
