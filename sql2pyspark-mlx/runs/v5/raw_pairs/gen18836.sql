SELECT
    dim.account_type,
    dim.status,
    src.merchant,
    COUNT(*) AS rows_total,
    SUM(src.amount) AS total_amount,
    GROUPING_ID() AS grouping_level
FROM transactions src
LEFT JOIN accounts dim ON src.account_id = dim.id
GROUP BY CUBE (dim.account_type, dim.status, src.merchant)
ORDER BY account_type NULLS LAST
