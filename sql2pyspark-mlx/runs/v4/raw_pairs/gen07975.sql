select id, country, rating, coalesce(supplier_name, 'unassigned') as supplier_name_filled from suppliers where city in ('New York', 'London') order by rating desc, id limit 10;
