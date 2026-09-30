select i.id, i.product_id, i.store_id, p.stock_qty, p.brand, p.price, coalesce(i.quantity, 0) as quantity_filled from inventory i left join products p on i.product_id = p.id
