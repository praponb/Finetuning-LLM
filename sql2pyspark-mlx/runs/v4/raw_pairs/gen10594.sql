with base as (
    select *
    from products
    where brand = 'Acme' and launch_date > '2024-04-22' and launch_date <= '2025-10-14'
),
summary as (
    select supplier_id, max(price) - min(price) as price_range, count(case when category = 'Grocery' then 1 end) as grocery_count
    from base
    group by supplier_id
)
select *
from summary
order by supplier_id
