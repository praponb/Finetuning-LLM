SELECT order_id, status FROM shipments INTERSECT SELECT order_id, status FROM shipments WHERE YEAR(delivered_date) = 2023;
