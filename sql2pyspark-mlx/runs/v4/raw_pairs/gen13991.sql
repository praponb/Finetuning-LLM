select id, rating, country from suppliers where rating <= (select min(rating) from suppliers where length(supplier_name) > 14) order by id desc;
