select *
from suppliers
where supplier_name not like 'S%' and city in ('Singapore', 'London')
order by rating desc, id desc
