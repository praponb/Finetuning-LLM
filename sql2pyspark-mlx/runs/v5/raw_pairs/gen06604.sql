SELECT
    c.city,
    src.account_type,
    COUNT(*) AS num_rows,
    MAX(src.balance) AS max_balance,
    GROUPING_ID() AS grouping_level
FROM accounts src
INNER JOIN customers c ON src.customer_id = c.id
GROUP BY CUBE (c.city, src.account_type);
