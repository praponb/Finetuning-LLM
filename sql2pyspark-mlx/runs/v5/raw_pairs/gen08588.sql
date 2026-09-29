select s.rating, t.price_count
from suppliers s
left join (select supplier_id, count(price) as price_count
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.price_count > 4
