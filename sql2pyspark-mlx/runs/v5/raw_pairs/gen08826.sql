select id, product_id
from inventory
where last_restock >= '2025-09-01' and quantity < 159 and quantity between 211 and 216;
