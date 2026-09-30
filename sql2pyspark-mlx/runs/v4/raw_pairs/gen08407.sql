SELECT QUARTER(signup_date) AS signup_quarter, ROUND(AVG(age), 2) AS avg_age, MAX(signup_date) AS latest_signup_date
FROM customers
WHERE segment IN ('Consumer', 'SMB')
GROUP BY signup_quarter
ORDER BY signup_quarter DESC;
