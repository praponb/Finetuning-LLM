select product_id
from inventory
where product_id = 2
except
select product_id
from inventory
where store_id in (3, 4, 5, 6);
