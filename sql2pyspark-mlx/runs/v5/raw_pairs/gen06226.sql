select id, country, city, rating, lead(rating, 1) over (partition by country, city order by rating, id) as next_rating from suppliers where upper(trim(supplier_name)) = 'GLOBAL SUPPLY';
