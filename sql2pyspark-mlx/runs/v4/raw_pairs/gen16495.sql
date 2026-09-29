select id, rating, supplier_name, country from suppliers where rating > (select max(rating) from suppliers where supplier_name like '%Wholesal%');
