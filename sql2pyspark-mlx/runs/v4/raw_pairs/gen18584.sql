SELECT COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count, SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit FROM customers WHERE name LIKE '%Chaiya%'
