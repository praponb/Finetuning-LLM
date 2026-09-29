select store_id, count(*) as row_count
from inventory
group by store_id
