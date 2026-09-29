select
    city,
    case when rating >= 4.2 then 'high' when rating >= 1.4 then 'mid' else 'low' end as rating_bucket,
    min(rating) as min_rating,
    max(rating) as max_rating
from suppliers
group by city, case when rating >= 4.2 then 'high' when rating >= 1.4 then 'mid' else 'low' end
having max(rating) < 2.1
order by city, max_rating desc, rating_bucket
limit 1
