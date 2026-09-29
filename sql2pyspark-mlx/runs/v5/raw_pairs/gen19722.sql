SELECT is_flagged, ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada
FROM transactions
WHERE merchant = 'Shell' AND account_id IN (13, 40)
GROUP BY 1
ORDER BY pct_lazada
