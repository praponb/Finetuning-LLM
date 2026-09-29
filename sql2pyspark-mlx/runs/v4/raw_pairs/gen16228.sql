with ranked as (
    select *, row_number() over (partition by store_id order by revenue desc, id) as rnk
    from store_sales
)
select
    id,
    store_id,
    revenue,
    product_id
from ranked
where rnk <= 3;
