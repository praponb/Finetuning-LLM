select id, city
from suppliers
where supplier_name like 'L%'
union
select id, city
from suppliers
where rating <= 3.8;
