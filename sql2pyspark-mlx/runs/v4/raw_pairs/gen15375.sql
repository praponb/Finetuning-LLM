select i.last_restock, i.quantity, p.price, s.rating from inventory i left join products p on i.product_id = p.id full outer join suppliers s on p.supplier_id = s.id
