select id, last_restock, quantity
from inventory
where last_restock = (select max(last_restock) from inventory where product_id in (2, 3, 7, 9));
