SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_smb, ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_corporate
FROM customers
GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
ORDER BY signup_month
