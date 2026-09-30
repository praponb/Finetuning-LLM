select s.rating, p.brand from suppliers s inner join products p on p.supplier_id = s.id where s.city = 'Singapore' and p.stock_qty <= 377;
