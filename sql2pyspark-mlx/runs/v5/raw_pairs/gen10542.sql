SELECT is_flagged, SUM(amount) total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM transactions WHERE txn_type <> 'CREDIT' GROUP BY is_flagged ORDER BY rnk
