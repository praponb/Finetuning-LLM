SELECT * FROM (SELECT method, status, amount FROM payments) PIVOT (SUM(amount) FOR status IN ('FAILED', 'SUCCESS'))
