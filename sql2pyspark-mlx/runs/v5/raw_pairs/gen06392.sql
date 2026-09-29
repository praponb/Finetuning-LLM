select id, status, order_id, date_sub(delivered_date, 22) as delivered_date_minus_22d from shipments where cost >= 82 or delivered_date > '2024-10-01' order by id;
