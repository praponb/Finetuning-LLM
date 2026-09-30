select s.city, s.rating, s.country, p.price, p.supplier_id from suppliers s inner join products p on p.supplier_id = s.id where s.rating <= 2.8 order by rating desc
