select s.region, round(stddev(i.quantity), 2) as stddev_quantity
from inventory i
inner join stores s on i.store_id = s.id
inner join products p on i.product_id = p.id
group by s.region
having round(stddev(i.quantity), 2) >= 64
order by region desc
limit 5;
