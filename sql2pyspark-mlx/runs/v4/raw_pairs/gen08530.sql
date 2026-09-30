select case when discount >= 0.37 then 'high' when discount >= 0.12 then 'mid' else 'low' end as discount_bucket, max(order_date) as latest_order_date
from orders
group by case when discount >= 0.37 then 'high' when discount >= 0.12 then 'mid' else 'low' end
order by latest_order_date asc, discount_bucket asc
limit 3;
