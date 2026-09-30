with ranked as (
    select
        id,
        store_id,
        sale_date,
        revenue,
        row_number() over (partition by store_id order by sale_date, id) as rn
    from store_sales
)
select *
from ranked
where rn <= 3
order by store_id desc
