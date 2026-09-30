select supplier_name, city
from suppliers
where city not in ('Berlin', 'New York', 'Bangkok')
union
select supplier_name, city
from suppliers
where country in ('JP', 'DE');
