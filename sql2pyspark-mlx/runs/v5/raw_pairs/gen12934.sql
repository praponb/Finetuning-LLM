SELECT status FROM orders WHERE discount < 0.16 INTERSECT SELECT status FROM orders WHERE status = 'COMPLETED';
