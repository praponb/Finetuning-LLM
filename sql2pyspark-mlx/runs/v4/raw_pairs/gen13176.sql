select p.brand, p.stock_qty, s.city, s.country, s.rating from products p inner join suppliers s on p.supplier_id = s.id where p.stock_qty between 198 and 367
