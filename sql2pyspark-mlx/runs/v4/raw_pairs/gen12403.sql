select id, rating, country, dense_rank() over (order by rating desc) as dense_rnk, max(rating) over (partition by country) as country_max_rating from suppliers order by country_max_rating;
