select
    id,
    sale_date,
    store_id,
    product_id
from store_sales
where sale_date = (select max(sale_date) from store_sales);
