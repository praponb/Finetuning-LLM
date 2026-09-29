select s.city, s.supplier_name, s.rating, p.category from suppliers s join products p on s.id = p.supplier_id where s.city = 'Berlin'
