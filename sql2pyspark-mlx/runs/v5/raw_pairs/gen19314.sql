with filtered as (
    select *, round(revenue, 0) as revenue_rounded
    from store_sales
    where sale_date >= to_date('2023-07-04')
)
select
    id,
    product_id,
    sale_date,
    revenue_rounded
from (
    select *, row_number() over (partition by store_id, product_id order by revenue desc, id) as dedup_rank
    from filtered
) x
where dedup_rank = 1;
