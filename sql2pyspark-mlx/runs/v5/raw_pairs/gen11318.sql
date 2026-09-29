select count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count
from suppliers
where country in ('SG', 'UK', 'DE') and city = 'New York'
order by chiang_mai_count
