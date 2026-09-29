select s.id, s.region, s.opened_date
from stores s
where s.city = 'Chiang Mai' and exists (select 1 from store_sales ss where ss.store_id = s.id)
