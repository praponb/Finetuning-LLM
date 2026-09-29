select id, supplier_name, city, round(rating, 1) as rating_rounded from suppliers where rating > 3.8 order by 4 desc, id limit 1;
