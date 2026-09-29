select s.supplier_name, s.rating, p.stock_qty, p.brand, p.launch_date from suppliers s inner join products p on p.supplier_id = s.id where p.product_name like 'Run%';
