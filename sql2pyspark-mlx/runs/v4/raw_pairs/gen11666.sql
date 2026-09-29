select a.id, a.category, a.stock_qty, a.price from products a where a.stock_qty = (select max(b.stock_qty) from products b where b.category = a.category)
