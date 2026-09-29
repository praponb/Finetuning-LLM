select id, sale_date, product_id
from store_sales
where units between 13 and 19
order by product_id;
