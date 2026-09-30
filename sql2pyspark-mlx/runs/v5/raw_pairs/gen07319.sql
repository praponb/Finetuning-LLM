select
    i.id,
    i.last_restock,
    i.product_id,
    s.region,
    p.price
from inventory i
inner join stores s on s.id = i.store_id
full outer join products p on i.product_id = p.id
where size(p.tags) > 1
order by price desc;
