WITH Ranked AS (SELECT id, status, amount, RANK() OVER (PARTITION BY status ORDER BY amount DESC) pos FROM payments) SELECT * FROM Ranked WHERE pos <= 3 ORDER BY pos DESC;
