SELECT is_active, COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count FROM customers GROUP BY 1 ORDER BY smb_count ASC
