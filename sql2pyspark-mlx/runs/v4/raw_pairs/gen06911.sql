SELECT delivered_date FROM shipments EXCEPT SELECT opened_date FROM stores ORDER BY delivered_date;
