SELECT * FROM (SELECT id, status, amount, DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC) pos FROM payments) ranked WHERE pos <= 3;
