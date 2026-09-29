select id, country, rating, sum(rating) over (partition by country order by rating, id) as running_rating from suppliers order by id;
