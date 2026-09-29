with ranked_store_sales as (
    select *, rank() over (partition by product_id order by sale_date desc) as rn
    from store_sales
)
select *
from ranked_store_sales
where rn <= 5;
