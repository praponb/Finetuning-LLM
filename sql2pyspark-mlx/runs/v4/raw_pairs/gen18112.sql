SELECT s.shipped_date, o.status, o.shipping_fee, st.opened_date FROM shipments s INNER JOIN orders o ON o.id = s.order_id INNER JOIN stores st ON o.store_id = st.id
