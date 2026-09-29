WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount >= 1700
        )
)
-- Multi-level aggregation
SELECT
    status,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(AVG(balance), 2) AS avg_balance_2,
    RANK() OVER (PARTITION BY status ORDER BY ROUND(AVG(balance), 2) DESC) AS avg_balance_2_rank
FROM kept
GROUP BY 1, 2, 3
ORDER BY avg_balance
