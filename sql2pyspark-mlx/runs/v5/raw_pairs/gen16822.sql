with top_store_sales as (
    select
        id,
        store_id,
        sale_date,
        product_id,
        row_number() over (partition by store_id order by sale_date desc, id) as rnk
    from store_sales
)
select *
from top_store_sales
where rnk <= 1
order by sale_date desc;
