select p.id, p.supplier_id, p.category from products p where p.id in (select product_id from order_items) order by p.supplier_id, id limit 5;
