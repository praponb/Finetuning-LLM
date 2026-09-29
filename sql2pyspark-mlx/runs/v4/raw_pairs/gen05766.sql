select case when price >= 1110 then 'high' when price >= 440 then 'mid' else 'low' end as price_bucket, avg(cost) as avg_cost
from products
group by price_bucket
having avg(cost) < 360
order by avg_cost;
