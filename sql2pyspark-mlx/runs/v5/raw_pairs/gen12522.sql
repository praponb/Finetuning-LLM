WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.account_type,
        e.merchant,
        COUNT(*) AS rows_total,
        ROUND(SUM(e.amount), 2) AS total_amount
    FROM transactions e
    LEFT JOIN accounts d ON e.account_id = d.id
    GROUP BY ROLLUP (d.account_type, e.merchant)
)
SELECT
    account_type,
    rows_total,
    total_amount
FROM rollup_totals
ORDER BY rows_total NULLS FIRST, account_type DESC
