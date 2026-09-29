select store_id, min(units) min_units
from store_sales
group by store_id
order by store_id, min_units
limit 3;
