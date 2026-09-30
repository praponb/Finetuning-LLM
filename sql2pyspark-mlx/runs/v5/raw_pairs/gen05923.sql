SELECT amount FROM payments WHERE order_id IN (6, 10, 29, 33) INTERSECT SELECT cost FROM shipments WHERE YEAR(shipped_date) >= 2023;
