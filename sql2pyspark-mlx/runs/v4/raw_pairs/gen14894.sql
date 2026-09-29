select p.id, p.cost, s.country from products p inner join suppliers s on s.id = p.supplier_id order by id desc, cost
