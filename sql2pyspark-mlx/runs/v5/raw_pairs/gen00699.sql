select s.rating, t.price_range
from suppliers s
inner join (select supplier_id, max(price) - min(price) as price_range
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.price_range >= 31
order by 2 desc;
