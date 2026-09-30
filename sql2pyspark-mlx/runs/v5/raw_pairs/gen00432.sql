select id
from products
intersect
select id
from products
where supplier_id is not null;
