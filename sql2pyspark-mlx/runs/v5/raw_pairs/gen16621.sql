SELECT id, order_id, paid_at FROM (SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS rn FROM payments WHERE HOUR(paid_at) BETWEEN 6 AND 11) AS r WHERE rn <= 3;
