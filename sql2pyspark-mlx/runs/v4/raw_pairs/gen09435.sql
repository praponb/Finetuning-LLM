select i.id, i.store_id, i.product_id, p.stock_qty, p.supplier_id from inventory i left join products p on i.product_id = p.id order by p.stock_qty desc;
