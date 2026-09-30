select
    id,
    product_id,
    sale_date,
    row_number() over (partition by product_id order by sale_date, id) sale_date_row
from store_sales
order by sale_date asc
