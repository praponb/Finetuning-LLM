WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'Priority' ELSE 'Other' END AS status_group,
        DATE_SUB(opened_date, 49) AS opened_date_minus_49d
    FROM accounts
    WHERE opened_date >= DATE '2023-07-16'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY customer_id) AS customer_id_min_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_avg_5,
        STDDEV_SAMP(balance) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_balance,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM base
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date_minus_49d DESC, id) = 1
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        status_group,
        opened_date_minus_49d,
        customer_id_min_balance,
        balance_avg_5,
        customer_id_stddev_samp_balance,
        last_known_balance,
        SUM(balance) OVER (PARTITION BY status, customer_id ORDER BY last_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY status ORDER BY balance_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        SUM(balance_avg_5) OVER (PARTITION BY status ORDER BY opened_date_minus_49d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_avg_5
    FROM latest
)
-- Grouped metrics
SELECT
    status,
    COUNT(*) AS row_count,
    MAX(balance_avg_5) FILTER (WHERE status = 'CLOSED') AS closed_max_balance_avg_5,
    ROUND(AVG(running_balance_avg_5), 2) AS avg_running_balance_avg_5,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    COUNT(*) FILTER (WHERE account_type = 'LOAN') AS loan_rows,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM with_windows_accounts
GROUP BY status
HAVING MAX(balance_avg_5) FILTER (WHERE status = 'CLOSED') >= 0
ORDER BY savings_count
