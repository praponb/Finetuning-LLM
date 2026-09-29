SELECT status, carrier FROM shipments WHERE cost >= 69 EXCEPT SELECT status, carrier FROM shipments WHERE MONTH(shipped_date) IN (2, 7, 11) ORDER BY status;
