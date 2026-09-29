SELECT id, email, signup_date, segment, CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok FROM customers WHERE is_active AND segment IN ('Corporate', 'Consumer', 'SMB')
