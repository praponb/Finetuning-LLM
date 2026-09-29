SELECT t.id, t.merchant FROM transactions t WHERE NOT t.is_flagged AND t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id = 13) ORDER BY t.id DESC LIMIT 10
