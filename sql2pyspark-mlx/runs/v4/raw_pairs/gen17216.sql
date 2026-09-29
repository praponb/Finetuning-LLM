select p.id, p.brand, p.cost, p.stock_qty, s.rating, s.city from products p inner join suppliers s on s.id = p.supplier_id where s.rating > 4.2 order by brand desc, id limit 20
