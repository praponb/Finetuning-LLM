select
    i.id,
    i.last_restock,
    p.launch_date,
    s.city
from inventory i
inner join products p on p.id = i.product_id
left outer join suppliers s on p.supplier_id = s.id
where i.quantity = 123
