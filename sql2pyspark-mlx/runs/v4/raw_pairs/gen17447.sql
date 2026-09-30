select
    id,
    product_id,
    revenue,
    row_number() over (partition by product_id order by revenue desc, id) row_num
from store_sales
where store_id in (1, 3);
