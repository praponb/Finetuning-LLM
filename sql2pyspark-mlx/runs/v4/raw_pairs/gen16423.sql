SELECT is_flagged, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) starbucks_count FROM transactions GROUP BY is_flagged HAVING SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) > 66;
