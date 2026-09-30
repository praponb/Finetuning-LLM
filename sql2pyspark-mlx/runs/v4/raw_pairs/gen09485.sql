select *, coalesce(supplier_name, 'unassigned') as supplier_name_filled from suppliers where country in ('UK', 'JP') order by rating, id limit 10;
