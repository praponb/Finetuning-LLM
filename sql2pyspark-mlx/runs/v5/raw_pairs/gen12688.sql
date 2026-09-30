SELECT
    d.status,
    src.merchant,
    COUNT(*) AS rows_total,
    MIN(src.amount) AS min_amount
FROM transactions src
INNER JOIN accounts d ON src.account_id = d.id
GROUP BY CUBE (d.status, src.merchant)
ORDER BY merchant DESC NULLS LAST
