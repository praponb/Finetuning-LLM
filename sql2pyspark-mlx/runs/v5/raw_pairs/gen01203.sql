select s.supplier_name, s.rating, p.stock_qty from suppliers s left join products p on p.supplier_id = s.id where s.supplier_name not like 'Pac%' order by p.stock_qty desc, s.supplier_name desc
