select country, sum(case when city = 'Tokyo' then 1 else 0 end) as tokyo_count, min(rating) as min_rating, count(*) as cnt from suppliers group by country;
