select sale_date, product_id
from store_sales
where revenue >= 1700
union all
select sale_date, product_id
from store_sales
where year(sale_date) = 2023;
