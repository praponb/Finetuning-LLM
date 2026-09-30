SELECT CASE WHEN country IN ('JP', 'SG') THEN 'core' ELSE 'other' END AS country_group, SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) AS th_count
FROM customers
GROUP BY 1;
