select id, store_id, round(quantity * 100, 2) as quantity_scaled
from inventory
where store_id = 4 and store_id = 6 and product_id in (4, 8)
