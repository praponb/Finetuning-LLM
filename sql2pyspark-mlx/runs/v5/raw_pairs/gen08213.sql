select id, rating, city from suppliers where rating >= (select max(rating) from suppliers)
