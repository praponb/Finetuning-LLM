WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(balance * 1.07, 2) adjusted_balance
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-05-06')
      AND status IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= '2025-09-01'
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        adjusted_balance,
        CASE WHEN balance >= 59000 THEN 'High' WHEN balance >= 6000 THEN 'Medium' ELSE 'Low' END balance_band,
        ROUND(adjusted_balance * 100, 2) adjusted_balance_adjusted
    FROM eligible
    WHERE opened_date >= DATE '2023-03-05'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        balance,
        opened_date,
        adjusted_balance,
        adjusted_balance_adjusted,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance,
        SUM(adjusted_balance) OVER (PARTITION BY customer_id, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_adjusted_balance,
        FIRST_VALUE(adjusted_balance, TRUE) OVER (PARTITION BY status ORDER BY adjusted_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_adjusted_balance,
        SUM(adjusted_balance_adjusted) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_adjusted_balance_adjusted,
        RANK() OVER (PARTITION BY status ORDER BY adjusted_balance NULLS FIRST) adjusted_balance_rank
    FROM valid
    WHERE customer_id IS NULL
),
base_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(adjusted_balance_adjusted, balance) least_adjusted_balance_adjusted_balance
    FROM enriched
    WHERE opened_date >= TO_DATE('2023-09-08')
      AND first_known_adjusted_balance <= 25
),
valid_accounts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        balance,
        opened_date,
        adjusted_balance,
        adjusted_balance_adjusted,
        running_balance,
        running_adjusted_balance,
        cumulative_adjusted_balance_adjusted,
        adjusted_balance_rank,
        least_adjusted_balance_adjusted_balance,
        adjusted_balance + adjusted_balance_adjusted adjusted_balance_plus_adjusted_balance_adjusted
    FROM base_accounts
    WHERE adjusted_balance < 40
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid_accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
)
SELECT
    id,
    opened_date,
    adjusted_balance,
    adjusted_balance_adjusted,
    running_balance,
    cumulative_adjusted_balance_adjusted,
    least_adjusted_balance_adjusted_balance,
    adjusted_balance_plus_adjusted_balance_adjusted
FROM not_flagged
ORDER BY opened_date;
