select
    i.id,
    i.last_restock,
    i.store_id,
    p.supplier_id,
    s.city,
    s.rating
from inventory i
inner join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where p.brand in ('Initech', 'Globex') and i.quantity <= 200;
