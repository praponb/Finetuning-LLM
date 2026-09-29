SELECT s.store_name, o.order_date, o.discount, o.shipping_fee FROM stores s LEFT JOIN orders o ON s.id = o.store_id WHERE o.store_id IN (9, 12, 19, 33)
