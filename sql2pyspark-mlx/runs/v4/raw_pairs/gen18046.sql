with ranked_store_sales as (
    select
        id,
        store_id,
        units,
        product_id,
        row_number() over (partition by store_id order by units, id) as rn
    from store_sales
)
select *
from ranked_store_sales
where rn <= 2
order by rn desc;
