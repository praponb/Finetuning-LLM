select p.id, p.cost, p.price, p.launch_date, s.supplier_name from products p inner join suppliers s on p.supplier_id = s.id order by p.id desc, cost desc;
