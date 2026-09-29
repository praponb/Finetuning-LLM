select ss.id, ss.units, s.store_name
from store_sales ss
left join stores s on ss.store_id = s.id
where s.city = 'Chiang Mai' or ss.units = 38;
