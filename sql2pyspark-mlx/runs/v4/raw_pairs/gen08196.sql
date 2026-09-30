select count(*) store_sale_count
from store_sales
where sale_date between '2023-05-23' and '2024-07-01'
order by store_sale_count desc
