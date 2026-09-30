with top_store_sales as (
    select *, row_number() over (partition by product_id order by revenue desc, id) as pos
    from store_sales
)
select
    id,
    product_id,
    revenue,
    units
from top_store_sales
where pos <= 1
