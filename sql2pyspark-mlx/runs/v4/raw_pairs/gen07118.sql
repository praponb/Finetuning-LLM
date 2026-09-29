select s.id, s.supplier_name from suppliers s where s.id in (select supplier_id from products) order by s.supplier_name desc
