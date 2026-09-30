select id, quantity, product_id
from inventory
where quantity >= (select max(quantity) from inventory)
