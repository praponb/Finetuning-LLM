SELECT CASE WHEN c.country IN ('TH', 'US') THEN 'core' ELSE 'other' END AS country_group, ROUND(STDDEV(a.balance), 2) AS stddev_balance
FROM accounts a
JOIN customers c ON c.id = a.customer_id
WHERE a.status IS NOT NULL
GROUP BY CASE WHEN c.country IN ('TH', 'US') THEN 'core' ELSE 'other' END
ORDER BY country_group ASC;
