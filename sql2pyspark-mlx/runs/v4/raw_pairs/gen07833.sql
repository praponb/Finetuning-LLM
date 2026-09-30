select
    id,
    sale_date,
    store_id,
    floor(revenue) revenue_whole,
    coalesce(revenue, 0) revenue_filled
from store_sales
where store_id in (1, 2, 4)
order by sale_date desc, id
limit 10
