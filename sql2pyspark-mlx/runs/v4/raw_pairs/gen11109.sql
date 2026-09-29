SELECT status, APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles, COUNT(*) AS row_count FROM accounts GROUP BY status ORDER BY status DESC NULLS FIRST;
