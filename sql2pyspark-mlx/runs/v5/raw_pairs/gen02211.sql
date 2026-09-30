SELECT is_flagged, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count, COUNT(amount) AS amount_count FROM transactions GROUP BY is_flagged HAVING amount_count < 4 ORDER BY fee_count DESC;
