select store_id, id
from store_sales
except
select store_id, id
from store_sales
where units <= 21;
