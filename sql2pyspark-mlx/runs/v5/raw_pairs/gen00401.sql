SELECT segment, city, SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit FROM customers WHERE credit_limit BETWEEN 6000 AND 13100 GROUP BY segment, city ORDER BY 2;
