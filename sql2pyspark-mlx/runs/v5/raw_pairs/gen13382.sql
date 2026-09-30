select
    i.id,
    i.last_restock,
    s.region,
    p.product_name,
    p.supplier_id
from inventory i
inner join stores s on i.store_id = s.id
full outer join products p on i.product_id = p.id
where i.last_restock between '2023-04-01' and '2023-05-01'
order by supplier_id, last_restock
