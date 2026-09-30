select s.supplier_name, s.rating, s.country, p.launch_date, p.cost from suppliers s left outer join products p on p.supplier_id = s.id where s.city = 'Berlin' and p.cost >= 400
