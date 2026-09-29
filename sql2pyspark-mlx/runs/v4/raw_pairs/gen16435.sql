select category, case when price >= 1050 then 'high' when price >= 590 then 'mid' else 'low' end as price_bucket, count(*) as product_count
from products
where launch_date between '2025-06-01' and '2025-07-20'
group by category, case when price >= 1050 then 'high' when price >= 590 then 'mid' else 'low' end
having count(*) >= 3
order by 1 desc, price_bucket asc
limit 1
