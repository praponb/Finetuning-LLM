select
    id,
    store_id,
    sale_date,
    units
from (
    select *, rank() over (partition by store_id, product_id order by sale_date desc nulls first) as latest_rank
    from store_sales
) x
where latest_rank = 1;
