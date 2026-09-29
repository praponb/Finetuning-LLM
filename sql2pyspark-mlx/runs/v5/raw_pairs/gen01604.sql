SELECT s.id, s.delivered_date, s.carrier, o.shipping_fee, st.region, st.sqft FROM shipments s LEFT JOIN orders o ON s.order_id = o.id INNER JOIN stores st ON st.id = o.store_id
