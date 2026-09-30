SELECT SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END) AS amazon_count
FROM transactions
WHERE merchant = 'Shell' AND account_id IN (27, 40);
