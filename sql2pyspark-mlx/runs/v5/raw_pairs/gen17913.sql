select city, sum(rating) as total_rating, count(*) as cnt, dense_rank() over (order by sum(rating) desc) as rnk from suppliers group by city;
