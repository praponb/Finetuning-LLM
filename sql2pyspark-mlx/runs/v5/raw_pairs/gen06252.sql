SELECT * FROM (SELECT status, method, amount FROM payments) PIVOT (AVG(amount) FOR method IN ('paypal', 'cash', 'card')) ORDER BY status;
