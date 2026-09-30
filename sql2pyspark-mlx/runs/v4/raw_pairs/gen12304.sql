SELECT is_flagged, COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) amazon_count, MIN(amount) min_amount FROM transactions GROUP BY is_flagged
