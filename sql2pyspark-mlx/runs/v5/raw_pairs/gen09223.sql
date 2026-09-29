select s.city, s.country, s.supplier_name, p.brand from suppliers s inner join products p on p.supplier_id = s.id where s.city in ('London', 'Berlin');
