select
    id,
    revenue,
    units,
    product_id
from store_sales
where product_id in (4, 10) and revenue <= 1200;
