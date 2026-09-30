select id, carrier, delivered_date, order_id from shipments where shipped_date >= '2024-08-01' and order_id is not null;
