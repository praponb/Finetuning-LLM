select o.id, o.discount, o.shipping_fee from orders o where o.status = 'CANCELLED' and o.store_id in (select id from stores where region not in ('East', 'West', 'South'));
