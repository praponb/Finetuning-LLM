SELECT is_flagged, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total FROM transactions GROUP BY is_flagged ORDER BY 3, total_amount DESC
