select case when rating >= 3.5 then 'high' when rating >= 1.9 then 'mid' else 'low' end as rating_bucket, sum(rating) as total_rating, round(sum(rating) * 100.0 / sum(sum(rating)) over (), 2) as pct_of_total
from suppliers
group by case when rating >= 3.5 then 'high' when rating >= 1.9 then 'mid' else 'low' end
