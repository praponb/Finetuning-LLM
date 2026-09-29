select
    i.id,
    i.last_restock,
    i.product_id,
    i.store_id
from inventory i
where i.product_id in (select id from products where stock_qty between 105 and 211);
