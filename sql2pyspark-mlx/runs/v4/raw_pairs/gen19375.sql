select o.id, o.shipping_fee from orders o where o.store_id not in (select id from stores where region = 'Central');
