select case when price >= 1350 then 'high' when price >= 580 then 'mid' else 'low' end as price_bucket, count(distinct cost) as unique_categorys
from products
where cost between 230 and 410
group by 1
having count(distinct cost) < 2
order by 1
