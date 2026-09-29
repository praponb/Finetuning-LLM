WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(status, 1, 3) AS status_prefix
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-06-12')
      AND opened_date BETWEEN '2024-05-01' AND '2025-03-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        status,
        status_prefix,
        SUM(balance) OVER w_grp AS running_balance,
        NTILE(4) OVER w_part AS balance_bucket,
        FIRST_VALUE(balance) IGNORE NULLS OVER w_grp AS first_known_balance
    FROM base
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY customer_id, status ORDER BY balance, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY first_known_balance, id) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < '2025-04-08'
        )
)
SELECT
    id,
    account_type,
    status,
    running_balance,
    balance_bucket,
    first_known_balance,
    CASE WHEN account_type RLIKE '^(CREDIT|LOAN)$' THEN 'PRIORITY' WHEN account_type RLIKE '^(SAVINGS|CHECKING)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS account_type_zone
FROM eligible
