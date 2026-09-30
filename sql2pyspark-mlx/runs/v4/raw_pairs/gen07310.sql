SELECT * FROM (SELECT method, status, amount FROM payments) PIVOT (COUNT(amount) FOR status IN ('REFUNDED', 'FAILED', 'SUCCESS')) ORDER BY method;
