select
    store_id,
    count(*) as num_inventory,
    max(quantity) as max_quantity,
    count(quantity) as quantity_count
from inventory
group by store_id
having max(quantity) > 80
order by 3 desc
