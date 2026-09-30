select id, revenue
from store_sales
where revenue >= (select max(revenue) from store_sales where revenue < 2200)
