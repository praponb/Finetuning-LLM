select
    id,
    store_id,
    units,
    row_number() over (partition by store_id order by units, id) as units_row
from store_sales
where revenue < 2700
