SELECT customer_id, APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles, PERCENTILE_APPROX(balance, 0.5) AS balance_p50 FROM accounts GROUP BY 1
