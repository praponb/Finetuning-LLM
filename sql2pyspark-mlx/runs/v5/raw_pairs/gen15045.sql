SELECT status FROM accounts EXCEPT SELECT status FROM shipments WHERE shipped_date >= '2023-07-01' ORDER BY status;
