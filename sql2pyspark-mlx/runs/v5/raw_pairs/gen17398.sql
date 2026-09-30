with cleaned as (
    select
        id,
        store_id,
        sale_date,
        units,
        units - revenue as revenue_per_unit
    from store_sales
    where sale_date >= date '2023-03-14'
      and product_id = 2
)
select
    id,
    sale_date,
    units,
    revenue_per_unit
from cleaned
qualify rank() over (partition by store_id order by revenue_per_unit desc) = 1
