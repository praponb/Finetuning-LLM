SELECT a.id, a.balance
FROM accounts a
WHERE a.status IS NOT NULL AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant IN ('Grab', 'Starbucks', 'Lazada'));
