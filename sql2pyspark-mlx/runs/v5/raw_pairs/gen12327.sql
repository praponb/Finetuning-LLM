select
    id,
    store_id,
    sale_date,
    revenue
from (
    select *, row_number() over (partition by store_id order by sale_date desc, id) as rn
    from store_sales
) as t
where rn <= 5;
