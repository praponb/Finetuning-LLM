with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from products s
    where exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id in (1, 3, 30, 37)
        )
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.product_id in (6, 7, 10)
        )
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        latest.revenue,
        latest.units
    from not_flagged s
    join lateral (
            select c.revenue, c.units
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        category,
        brand,
        revenue,
        units
    from (
        select *, row_number() over (partition by brand, category order by launch_date, id) as rn
        from with_last
    ) ranked
    where rn = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id in (5, 20)
        )
)
-- Native SQL PIVOT
select *
from (
    select
        brand,
        category,
        revenue
    from kept
) src
pivot (
    avg(revenue) as amount
    for category in ('Grocery' as grocery, 'Home' as home)
);
