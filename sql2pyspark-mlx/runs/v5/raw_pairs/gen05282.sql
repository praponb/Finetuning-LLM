select supplier_id
from products
where supplier_id in (11, 24)
union all
select supplier_id
from products
where category = 'Sports'
order by supplier_id
