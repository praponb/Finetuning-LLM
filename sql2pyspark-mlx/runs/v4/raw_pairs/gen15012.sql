SELECT city, SUM(CASE WHEN city = 'Tokyo' THEN credit_limit ELSE 0 END) AS tokyo_credit_limit, APPROX_PERCENTILE(age, 0.9) AS age_p90 FROM customers GROUP BY city ORDER BY age_p90 DESC;
