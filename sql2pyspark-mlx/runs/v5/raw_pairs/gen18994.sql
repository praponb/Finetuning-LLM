select id, quantity, last_restock
from inventory
where product_id in (3, 6, 8) and quantity < 144
order by last_restock;
