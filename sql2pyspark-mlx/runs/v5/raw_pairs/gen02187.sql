SELECT account_id, amount, merchant
FROM transactions
WHERE amount < 2700 AND merchant IN ('Lazada', '7-Eleven');
