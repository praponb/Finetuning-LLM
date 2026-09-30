SELECT * FROM (SELECT method, order_id, status, amount FROM payments) src PIVOT (AVG(amount) AS value_sum FOR status IN ('REFUNDED', 'FAILED'))
