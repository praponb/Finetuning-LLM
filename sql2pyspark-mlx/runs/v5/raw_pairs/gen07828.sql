SELECT is_flagged, MAX(amount) - MIN(amount) AS amount_range
FROM transactions
GROUP BY is_flagged
ORDER BY amount_range, is_flagged
LIMIT 1;
