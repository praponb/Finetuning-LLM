WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        COALESCE(balance, 0) AS balance_filled,
        CASE WHEN account_type IN ('CREDIT', 'LOAN') THEN 'Group A' ELSE 'Other' END AS account_type_group,
        COALESCE(status, 'OTHER') AS status_clean
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status IS DISTINCT FROM 'FROZEN'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_filled >= 63 THEN 'HIGH' ELSE 'LOW' END AS balance_filled_tier,
    COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END) AS loan_count,
    COUNT(*) AS cnt,
    MAX(balance) FILTER (WHERE status = 'OPEN') AS open_max_balance,
    AVG(balance_filled) AS avg_balance_filled
FROM base
GROUP BY CASE WHEN balance_filled >= 63 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_balance_filled DESC NULLS LAST;
