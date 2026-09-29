SELECT opened_date FROM accounts WHERE balance > 45000 EXCEPT SELECT delivered_date FROM shipments WHERE MONTH(delivered_date) IN (1, 7, 9);
