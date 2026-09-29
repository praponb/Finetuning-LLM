SELECT channel FROM orders EXCEPT ALL SELECT channel FROM orders WHERE order_date BETWEEN '2024-09-01' AND '2025-09-11';
