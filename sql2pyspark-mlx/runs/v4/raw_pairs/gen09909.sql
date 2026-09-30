SELECT id, is_flagged
FROM transactions
WHERE merchant != 'Amazon' OR txn_time < '2024-08-27'
ORDER BY id DESC;
