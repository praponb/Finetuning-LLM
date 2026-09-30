select id, units
from store_sales
where sale_date between '2024-09-01' and '2024-09-27'
except
select id, units
from store_sales
where store_id = 3
order by units
