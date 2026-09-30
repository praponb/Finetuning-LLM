SELECT is_flagged, MAX(amount) AS max_amount FROM transactions WHERE merchant IN ('Grab', 'Amazon', 'Lazada') GROUP BY is_flagged HAVING MAX(amount) > 3200 ORDER BY max_amount;
