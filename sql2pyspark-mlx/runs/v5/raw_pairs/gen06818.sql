select i.store_id, p.supplier_id, s.rating, s.supplier_name from inventory i left join products p on i.product_id = p.id inner join suppliers s on p.supplier_id = s.id;
