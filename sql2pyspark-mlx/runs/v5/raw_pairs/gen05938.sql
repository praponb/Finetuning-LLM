select id, rating, country, city, coalesce(rating, 0) as rating_filled from suppliers order by rating desc, id desc;
