select s.city, s.country, t.stock_qty_count
from suppliers s
left join (select supplier_id, count(stock_qty) stock_qty_count
    from products
    where cost >= 530
    group by supplier_id) t on s.id = t.supplier_id
where t.stock_qty_count > 8;
