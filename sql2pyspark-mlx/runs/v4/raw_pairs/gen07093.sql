select s.id, s.supplier_name, t.unique_brands
from suppliers s
left outer join (select supplier_id, count(distinct stock_qty) as unique_brands
    from products
    where supplier_id in (2, 27)
    group by supplier_id) t on s.id = t.supplier_id
order by s.supplier_name
