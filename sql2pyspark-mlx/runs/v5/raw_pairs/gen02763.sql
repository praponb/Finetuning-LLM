select
    s.id,
    s.store_name,
    s.opened_date,
    s.region
from stores s
where s.store_name is not null and exists (select 1 from orders o where o.store_id = s.id)
order by s.opened_date, id
limit 20
