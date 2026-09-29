select
    s.supplier_name,
    s.rating,
    p.cost,
    p.supplier_id,
    p.brand
from suppliers s
left outer join products p on p.supplier_id = s.id
where array_contains(p.tags, 'bestseller') and p.product_name like 'W%'
order by rating
