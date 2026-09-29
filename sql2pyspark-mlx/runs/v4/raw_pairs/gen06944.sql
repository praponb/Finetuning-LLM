select id, country, city, rating, sum(rating) over (partition by country, city order by rating, id) as running_rating from suppliers
