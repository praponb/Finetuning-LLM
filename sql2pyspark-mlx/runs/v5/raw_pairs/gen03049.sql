SELECT is_flagged, avg_amount, min_amount FROM (SELECT is_flagged, ROUND(AVG(amount), 2) avg_amount, MIN(amount) min_amount FROM transactions GROUP BY is_flagged) x WHERE min_amount < 1100;
