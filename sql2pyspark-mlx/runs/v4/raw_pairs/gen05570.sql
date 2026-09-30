select
    id,
    product_id,
    revenue,
    row_number() over (partition by product_id order by revenue, id) as revenue_row
from store_sales
where revenue <= 1800
order by revenue_row desc
