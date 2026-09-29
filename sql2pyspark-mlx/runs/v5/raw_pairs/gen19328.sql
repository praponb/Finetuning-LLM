select country, sum(rating) as total_rating
from suppliers
where rating >= 3.1
group by country
order by total_rating, country
limit 50;
