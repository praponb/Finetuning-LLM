SELECT CASE WHEN country IN ('TH', 'SG') THEN 'priority' ELSE 'other' END AS country_group, SUM(credit_limit) AS total_credit_limit
FROM customers
GROUP BY CASE WHEN country IN ('TH', 'SG') THEN 'priority' ELSE 'other' END;
