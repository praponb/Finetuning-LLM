select max(revenue) - min(revenue) revenue_range, count(*) num_store_sales
from store_sales
order by num_store_sales desc
