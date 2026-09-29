SELECT * FROM (SELECT status, method, amount FROM payments) src PIVOT (SUM(amount) AS total FOR method IN ('cash', 'bank_transfer'));
