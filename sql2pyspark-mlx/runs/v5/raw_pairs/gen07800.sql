select
    id,
    product_id,
    units,
    sale_date
from (
    select *, row_number() over (partition by product_id order by units desc, id) as pos
    from store_sales
    where store_id = 3
) as t
where pos = 1
