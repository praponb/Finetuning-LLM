select ss.id, ss.store_id
from store_sales ss
where ss.revenue >= 3300 and ss.store_id in (select id from stores where sqft = 11368)
order by store_id desc, ss.id desc
