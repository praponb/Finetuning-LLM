select
    i.last_restock,
    p.price,
    p.cost,
    p.category
from inventory i
inner join products p on i.product_id = p.id;
