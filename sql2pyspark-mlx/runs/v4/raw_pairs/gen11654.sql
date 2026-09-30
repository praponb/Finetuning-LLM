select case when rating >= 3.9 then 'high' when rating >= 2.5 then 'mid' else 'low' end as rating_bucket, count(case when country = 'JP' then 1 end) as jp_count
from suppliers
where city = 'New York'
group by 1
order by 1 desc, jp_count;
