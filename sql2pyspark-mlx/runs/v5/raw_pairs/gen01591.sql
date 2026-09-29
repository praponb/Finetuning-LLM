SELECT a.status, SUM(CASE WHEN t.merchant = 'Lazada' THEN 1 ELSE 0 END) AS lazada_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE TO_DATE(t.txn_time) >= '2025-07-01'
GROUP BY a.status
ORDER BY a.status ASC
