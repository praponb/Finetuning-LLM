select last_restock, store_id, product_id
from inventory
where product_id in (1, 2, 5, 9) and quantity < 233 and product_id = 4;
