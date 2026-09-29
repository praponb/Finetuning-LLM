WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(status, ' ', account_type) AS status_account_type,
        CONCAT_WS('-', account_type, status) AS account_type_status_key
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-10-25')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date >= sub.start_date
       AND e.opened_date < sub.end_date + INTERVAL 14 DAYS
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY subscription_id NULLS FIRST, id, subscription_id) AS dedup_rank
        FROM with_window
    ) ranked
    WHERE dedup_rank = 1
),
base_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status_account_type,
        account_type_status_key,
        subscription_id,
        plan,
        monthly_fee,
        dedup_rank,
        CAST(monthly_fee AS INT) AS monthly_fee_int,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM latest
    WHERE opened_date > DATE '2023-10-03'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status_account_type,
        account_type_status_key,
        subscription_id,
        plan,
        monthly_fee,
        dedup_rank,
        monthly_fee_int,
        status_clean,
        SUM(monthly_fee) OVER (ORDER BY balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        NTILE(10) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id) AS monthly_fee_bucket,
        SUM(balance) OVER (PARTITION BY plan, customer_id ORDER BY balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        SUM(monthly_fee_int) OVER (PARTITION BY plan, account_type ORDER BY opened_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_int,
        SUM(subscription_id) OVER (PARTITION BY plan) AS plan_sum_subscription_id
    FROM base_accounts
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    account_type,
    MAX(balance) AS max_balance,
    SUM(CASE WHEN account_type = 'SAVINGS' THEN cumulative_balance ELSE 0 END) AS savings_cumulative_balance,
    RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date) ORDER BY MAX(balance) DESC) AS max_balance_rank
FROM metrics
GROUP BY DATE_TRUNC('YEAR', opened_date), account_type
