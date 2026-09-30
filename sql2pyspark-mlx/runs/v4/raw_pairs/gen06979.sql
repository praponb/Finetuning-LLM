SELECT s.status, s.cost, o.shipping_fee, st.store_name, st.sqft FROM shipments s JOIN orders o ON s.order_id = o.id JOIN stores st ON st.id = o.store_id;
