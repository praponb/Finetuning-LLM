select case when city in ('Chiang Mai', 'New York') then 'core' else 'other' end as city_group, sum(rating) as total_rating, rank() over (order by sum(rating) desc) as rating_rank
from suppliers
group by case when city in ('Chiang Mai', 'New York') then 'core' else 'other' end
order by city_group desc, total_rating desc;
