SELECT * FROM (SELECT method, status, amount FROM payments WHERE HOUR(paid_at) BETWEEN 9 AND 16) PIVOT (MAX(amount) FOR status IN ('SUCCESS', 'REFUNDED', 'FAILED'))
