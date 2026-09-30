select i.product_id, p.brand
from inventory i
left outer join products p on i.product_id = p.id
where i.quantity between 186 and 211 or i.store_id in (2, 4, 6);
