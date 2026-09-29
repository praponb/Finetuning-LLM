select store_id, max(units) as max_units, round(avg(units), 2) as avg_units
from store_sales
where units is not null
group by store_id
having avg_units > 29
