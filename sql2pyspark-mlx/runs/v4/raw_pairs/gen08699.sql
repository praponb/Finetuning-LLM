SELECT id, amount FROM orders WHERE channel IS NULL AND status IN ('SHIPPED', 'PENDING', 'COMPLETED') ORDER BY id;
