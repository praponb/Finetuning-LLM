SELECT MONTH(signup_date) AS signup_month, MIN(signup_date) AS min_signup_date
FROM customers
WHERE segment <> 'SMB'
GROUP BY MONTH(signup_date);
