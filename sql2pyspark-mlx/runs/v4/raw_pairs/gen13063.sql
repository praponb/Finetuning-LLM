SELECT is_flagged, SUM(amount) AS total_amount, COUNT(*) AS cnt, SUM(SUM(amount)) OVER (ORDER BY is_flagged) AS cumulative_amount FROM transactions GROUP BY is_flagged;
