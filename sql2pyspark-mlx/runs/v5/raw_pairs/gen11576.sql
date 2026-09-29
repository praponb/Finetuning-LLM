SELECT
    p.country,
    s.account_type,
    COUNT(*) AS num_rows,
    SUM(s.balance) AS total_balance
FROM accounts s
JOIN customers p ON s.customer_id = p.id
GROUP BY CUBE (p.country, s.account_type);
