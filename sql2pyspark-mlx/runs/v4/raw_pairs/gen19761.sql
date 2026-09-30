SELECT id, status FROM orders EXCEPT ALL SELECT id, status FROM orders WHERE discount >= 0.33;
