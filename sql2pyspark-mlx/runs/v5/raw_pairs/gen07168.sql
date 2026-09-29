select s.id, s.supplier_name, t.stock_qty_count
from suppliers s
join (select supplier_id, count(stock_qty) as stock_qty_count
    from products
    group by supplier_id) t on s.id = t.supplier_id
order by s.id desc
limit 5
