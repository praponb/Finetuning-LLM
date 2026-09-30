select i.id, i.last_restock, i.quantity, i.product_id from inventory i where i.store_id in (select id from stores where city = 'Tokyo') order by id desc;
