select
    id,
    store_id,
    product_id,
    cast(revenue as int) revenue_int
from store_sales
where units <> 24
