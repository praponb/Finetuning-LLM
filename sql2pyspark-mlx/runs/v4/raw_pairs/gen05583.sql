select p.id, p.brand, p.stock_qty from products p where p.brand = 'Stark' and not exists (select 1 from order_items oi where oi.product_id = p.id) order by brand
