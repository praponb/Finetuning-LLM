select id, rating from suppliers where rating >= (select avg(rating) from suppliers where city = 'Bangkok') order by id limit 1
