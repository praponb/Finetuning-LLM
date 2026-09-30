select 'store_sale' origin, sale_date
from store_sales
union
select 'product' origin, launch_date
from products
where cost < 310
order by sale_date;
