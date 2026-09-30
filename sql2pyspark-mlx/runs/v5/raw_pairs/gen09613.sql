SELECT a.id, a.balance, a.status FROM accounts a WHERE YEAR(a.opened_date) = 2025 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant IS NOT NULL)
