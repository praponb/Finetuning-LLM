SELECT cost FROM shipments WHERE YEAR(shipped_date) >= 2025 EXCEPT SELECT shipping_fee cost FROM orders WHERE order_date > '2023-10-01' ORDER BY cost DESC
