SELECT o.id, o.order_date, s.sqft, s.opened_date, o.amount / NULLIF(o.shipping_fee, 0) AS amount_per_shipping_fee FROM orders o LEFT OUTER JOIN stores s ON o.store_id = s.id;
