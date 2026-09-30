select case when rating >= 4.2 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket, round(avg(rating), 2) as avg_rating
from suppliers
where city in ('Chiang Mai', 'Berlin')
group by rating_bucket
order by rating_bucket;
