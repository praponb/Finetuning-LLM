select store_id
from orders
except all
select store_id
from orders
where channel = 'store'
