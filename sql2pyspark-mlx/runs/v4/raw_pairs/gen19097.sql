select s.id, s.supplier_name, t.max_stock_qty
from suppliers s
join (select supplier_id, max(stock_qty) as max_stock_qty
    from products
    where size(tags) >= 1
    group by supplier_id) t on s.id = t.supplier_id
where t.max_stock_qty >= 400
