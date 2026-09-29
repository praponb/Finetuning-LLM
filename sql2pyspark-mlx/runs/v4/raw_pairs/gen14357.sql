WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(balance, 1, 0) OVER (ORDER BY opened_date, id) AS next_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_6
    FROM accounts
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_avg_6 >= 68449 THEN 'HIGH' ELSE 'LOW' END AS balance_avg_6_tier,
    COUNT(*) FILTER (WHERE account_type = 'SAVINGS') AS savings_rows,
    COUNT(DISTINCT status) AS distinct_status,
    SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance
FROM with_windows
GROUP BY 1
ORDER BY savings_rows DESC NULLS LAST
