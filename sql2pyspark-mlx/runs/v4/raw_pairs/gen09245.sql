select supplier_name
from suppliers
where not (city not in ('Singapore', 'London', 'New York') and rating < 2.3)
order by supplier_name;
