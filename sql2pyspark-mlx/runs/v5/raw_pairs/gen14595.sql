SELECT is_flagged, txn_type, MAX(amount) - MIN(amount) AS amount_range FROM transactions GROUP BY 1, 2 HAVING MAX(amount) - MIN(amount) > 39 ORDER BY txn_type DESC;
