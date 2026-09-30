SELECT * FROM (SELECT method, status, amount FROM payments) PIVOT (COUNT(amount) FOR status IN ('REFUNDED', 'SUCCESS', 'FAILED'));
