SELECT s.carrier, s.delivered_date, o.channel, st.opened_date FROM shipments s JOIN orders o ON s.order_id = o.id JOIN stores st ON o.store_id = st.id WHERE st.store_name IS NOT NULL;
