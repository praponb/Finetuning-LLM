SELECT txn_type, COUNT(DISTINCT amount) AS unique_merchants FROM transactions GROUP BY txn_type HAVING unique_merchants >= 4 ORDER BY unique_merchants DESC;
