WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id = 36
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit,
        COALESCE(status, 'OTHER') AS status_filled
    FROM not_flagged
    WHERE customer_id IS NOT NULL
      AND opened_date >= DATE '2023-04-13'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        status,
        is_credit,
        status_filled,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        AVG(is_credit) OVER (PARTITION BY account_type ORDER BY is_credit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS is_credit_avg_5,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY account_type, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_2,
        SUM(is_credit) OVER (PARTITION BY customer_id, account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_credit,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY is_credit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance
    FROM filtered
    WHERE balance BETWEEN 35000 AND 40000
),
not_flagged_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount >= 2000
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_is_credit >= 311 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_is_credit_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
    COUNT(*) AS event_count,
    ROUND(AVG(first_known_balance), 2) AS avg_first_known_balance
FROM not_flagged_accounts
GROUP BY CUBE (CASE WHEN cumulative_is_credit >= 311 THEN 'LARGE' ELSE 'SMALL' END)
HAVING COUNT(*) FILTER (WHERE status = 'OPEN') < 1000
ORDER BY avg_first_known_balance NULLS LAST;
