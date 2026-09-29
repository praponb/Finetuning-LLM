select city, sum(rating) as total_rating, avg(rating) as avg_rating from suppliers group by city
