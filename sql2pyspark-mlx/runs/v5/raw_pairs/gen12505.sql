select a.id, a.category, a.cost, a.stock_qty from products a where a.cost = (select min(b.cost) from products b where b.category = a.category) order by a.cost desc;
