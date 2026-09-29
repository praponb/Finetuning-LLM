select
    s.id,
    s.country,
    s.city,
    s.supplier_name
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.category = 'Fashion')
order by s.country asc, s.id desc
