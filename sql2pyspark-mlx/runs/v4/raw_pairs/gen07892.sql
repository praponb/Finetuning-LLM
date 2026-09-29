select id, city, rating, avg(rating) over (partition by city) as city_avg_rating from suppliers where rating >= 2.8 order by rating, id
