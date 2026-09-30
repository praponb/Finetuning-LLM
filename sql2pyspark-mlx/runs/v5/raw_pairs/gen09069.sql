SELECT YEAR(signup_date) signup_year, SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) smb_count, COUNT(DISTINCT credit_limit) unique_citys
FROM customers
GROUP BY YEAR(signup_date)
ORDER BY smb_count DESC, signup_year DESC;
