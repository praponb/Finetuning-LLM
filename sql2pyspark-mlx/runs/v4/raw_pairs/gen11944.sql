WITH base AS (
    SELECT *, INITCAP(TRIM(account_type)) AS account_type_title
    FROM accounts
    WHERE opened_date > TO_DATE('2023-09-14')
      AND account_type IN ('CREDIT', 'LOAN', 'CHECKING')
),
deduped AS (
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY opened_date) = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY balance DESC, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
)
SELECT
    status,
    COUNT(DISTINCT status) AS distinct_status,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    MAX(balance) FILTER (WHERE account_type = 'CREDIT') AS credit_max_balance,
    MAX(balance) FILTER (WHERE status = 'OPEN') AS open_max_balance,
    GROUPING_ID() AS grouping_level
FROM ranked
GROUP BY CUBE (status)
ORDER BY open_max_balance NULLS LAST;
