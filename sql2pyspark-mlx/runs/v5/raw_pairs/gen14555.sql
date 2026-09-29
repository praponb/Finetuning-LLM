select s.region, s.city, count(ss.revenue) as revenue_count
from store_sales ss
right join stores s on s.id = ss.store_id
group by s.region, s.city;
