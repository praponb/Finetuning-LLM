select id, rating, supplier_name from suppliers where rating >= (select max(rating) from suppliers)
