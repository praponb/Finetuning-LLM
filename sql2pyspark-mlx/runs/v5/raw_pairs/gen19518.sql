select
    i.id,
    i.product_id,
    s.opened_date,
    p.supplier_id,
    p.cost
from inventory i
left join stores s on i.store_id = s.id
inner join products p on i.product_id = p.id
where i.quantity < 92;
