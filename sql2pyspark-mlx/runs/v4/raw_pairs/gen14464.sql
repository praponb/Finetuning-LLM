select s.region, count(ss.id) as store_sale_count
from stores s
left join store_sales ss on s.id = ss.store_id
group by s.region
