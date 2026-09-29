with base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        concat(city, ' ', country) as city_country
    from suppliers
    where city is not null
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.city,
        top1.brand,
        top1.category
    from base s
    left join lateral (
            select c.brand, c.category
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.brand,
        s.category,
        latest.stock_qty
    from with_last s
    left join lateral (
            select c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
),
base_suppliers as (
    -- Filter the raw rows and derive helper columns
    select *, round(stock_qty, 0) as stock_qty_rounded
    from latest_match
    where category is not null
)
select
    id,
    supplier_name,
    brand,
    category,
    stock_qty,
    stock_qty_rounded,
    case when category rlike '^(Sports|Grocery)$' then 'EXTENDED' else 'PRIORITY' end as category_zone
from base_suppliers
order by id desc nulls last
