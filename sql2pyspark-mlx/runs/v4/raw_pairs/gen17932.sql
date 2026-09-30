with base as (
    select
        *,
        case when price >= 1070 then 'High' when price >= 230 then 'Medium' else 'Low' end as price_level,
        coalesce(brand, 'UNKNOWN') as brand_clean
    from products
    where launch_date > date '2023-03-28'
      and supplier_id is not null
),
with_last as (
    select s.*, x.inventory_quantity
    from base s
    left join lateral (
            select c.quantity as inventory_quantity
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.revenue < 2200
        )
)
select
    category,
    date_trunc('month', launch_date) as activity_month,
    count(*) as num_rows,
    round(avg(stock_qty), 2) as avg_stock_qty
from not_flagged
group by category, date_trunc('month', launch_date)
order by num_rows desc;
