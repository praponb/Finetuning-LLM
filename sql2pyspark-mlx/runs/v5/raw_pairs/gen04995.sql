SELECT * FROM (SELECT method, status, amount FROM payments WHERE paid_at < '2024-06-08') PIVOT (COUNT(amount) FOR status IN ('REFUNDED', 'SUCCESS'));
