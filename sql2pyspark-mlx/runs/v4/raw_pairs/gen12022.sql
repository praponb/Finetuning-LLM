SELECT *
FROM (
    SELECT segment, QUARTER(signup_date) AS signup_quarter, SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END) AS smb_count
    FROM customers
    GROUP BY segment, QUARTER(signup_date)
) AS agg
WHERE smb_count > 42
ORDER BY segment ASC
