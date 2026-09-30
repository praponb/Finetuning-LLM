with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        price + stock_qty as price_plus_stock_qty,
        add_months(launch_date, 1) as launch_date_plus_months
    from products
    where cost between 500 and 580
      and launch_date > date '2023-10-02'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.unit_price < 160
        )
),
base as (
    -- Basic cleansing and defaults
    select *, datediff(launch_date, launch_date_plus_months) as days_launch_date_plus_months_to_launch_date
    from not_flagged
    where launch_date > date '2023-03-29'
      and brand is not null
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, top1.sale_date
    from base s
    join lateral (
            select c.sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) top1 on true
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, top1.store_sale_sale_date
    from with_last s
    left join lateral (
            select c.sale_date as store_sale_sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.order_id in (3, 21, 29)
        )
)
-- Native SQL PIVOT
select *
from (
    select
        brand,
        category,
        stock_qty
    from eligible
) src
pivot (
    sum(stock_qty) as total,
    count(1) as n
    for category in ('Sports', 'Books')
)
