SELECT * FROM (SELECT account_type, customer_id, status, balance FROM accounts) src PIVOT (AVG(balance) AS amount, COUNT(1) AS n FOR status IN ('FROZEN' AS frozen, 'CLOSED' AS closed));
