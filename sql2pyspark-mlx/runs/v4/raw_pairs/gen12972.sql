select case when rating >= 3.7 then 'high' when rating >= 1.5 then 'mid' else 'low' end as rating_bucket, avg(rating) as avg_rating
from suppliers
where rating <= 4.1 or rating < 3.5
group by case when rating >= 3.7 then 'high' when rating >= 1.5 then 'mid' else 'low' end;
