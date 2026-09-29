SELECT is_flagged, SUM(amount) total_amount
FROM transactions
GROUP BY is_flagged
ORDER BY total_amount DESC
