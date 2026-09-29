select
    ss.id,
    ss.revenue,
    ss.store_id,
    s.region,
    s.sqft,
    s.store_name
from store_sales ss
inner join stores s on ss.store_id = s.id
where s.store_name is not null
