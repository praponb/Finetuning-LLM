select suppliers.country, t.sports_count, t.stock_qty_count
from suppliers
left join (select supplier_id, sum(case when category = 'Sports' then 1 else 0 end) as sports_count, count(stock_qty) as stock_qty_count
    from products
    group by supplier_id) t on suppliers.id = t.supplier_id
order by suppliers.country, stock_qty_count
