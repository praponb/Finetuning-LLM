SELECT * FROM (SELECT method, status, amount FROM payments) src PIVOT (MAX(amount) AS value_sum FOR status IN ('REFUNDED', 'FAILED', 'SUCCESS'));
