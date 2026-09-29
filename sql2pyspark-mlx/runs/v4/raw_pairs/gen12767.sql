select id, shipped_date, carrier, cost from shipments where order_id in (1, 7, 20, 35) order by carrier;
