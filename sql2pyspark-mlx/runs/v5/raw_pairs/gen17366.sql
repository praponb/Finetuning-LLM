SELECT s.supplier_name, p.brand, p.cost, p.launch_date FROM suppliers s INNER JOIN products p ON s.id = p.supplier_id;
