select i.product_id, i.quantity, p.supplier_id, s.rating from inventory i join products p on i.product_id = p.id left join suppliers s on p.supplier_id = s.id;
