select
    city,
    case when rating >= 4.3 then 'high' when rating >= 2.2 then 'mid' else 'low' end as rating_bucket,
    round(stddev(rating), 2) as stddev_rating,
    round(avg(rating), 2) as avg_rating,
    count(rating) as rating_count
from suppliers
group by city, case when rating >= 4.3 then 'high' when rating >= 2.2 then 'mid' else 'low' end
order by city, stddev_rating desc, rating_bucket
limit 3;
