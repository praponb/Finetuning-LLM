select city, sum(rating) as total_rating, dense_rank() over (order by sum(rating) desc) as rnk from suppliers where rating < 4.1 group by city order by total_rating desc, rnk desc;
