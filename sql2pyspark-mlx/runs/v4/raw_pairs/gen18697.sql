SELECT a.id, a.status, COUNT(t.id) AS transaction_count FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE a.balance > 77000 GROUP BY a.id, a.status ORDER BY transaction_count ASC;
