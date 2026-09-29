select s.city, count(distinct ss.revenue) as unique_stores, sum(ss.revenue) as total_revenue
from store_sales ss
inner join stores s on ss.store_id = s.id
where ss.units != 12
group by s.city
order by total_revenue desc, unique_stores desc;
