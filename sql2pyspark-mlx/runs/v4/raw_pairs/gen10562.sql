select store_id, quantity
from inventory
where not (store_id in (2, 4, 5, 6) and store_id in (1, 3, 4));
