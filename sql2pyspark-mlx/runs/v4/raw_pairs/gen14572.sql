WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        LOWER(status) AS status_lower,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months
    FROM accounts
    WHERE status IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance >= 62000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    SUM(CASE WHEN account_type = 'CREDIT' THEN balance ELSE 0 END) AS credit_balance,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    COUNT_IF(account_type = 'LOAN') AS loan_events,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list_2,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list_3
FROM cleaned
GROUP BY CASE WHEN balance >= 62000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING credit_balance > 0
ORDER BY credit_balance DESC NULLS FIRST;
