SELECT country, COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count FROM customers GROUP BY country ORDER BY smb_count
