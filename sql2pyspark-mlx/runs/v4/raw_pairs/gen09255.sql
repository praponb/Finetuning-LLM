select country, city
from customers
where not is_active
union
select country, city
from suppliers
where supplier_name is null
order by country
