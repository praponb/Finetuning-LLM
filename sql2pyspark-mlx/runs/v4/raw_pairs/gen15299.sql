select delivered_date
from shipments
where order_id in (4, 5, 8, 16)
union
select sale_date
from store_sales
where revenue <= 1200;
