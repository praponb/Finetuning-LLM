select country, sum(case when city = 'London' then 1 else 0 end) as london_count, sum(rating) as total_rating
from suppliers
group by country
order by london_count asc, total_rating desc;
