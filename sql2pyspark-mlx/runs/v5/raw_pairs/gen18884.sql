WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_type,
        balance,
        opened_date
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id ASC) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        balance,
        SUM(balance) OVER (PARTITION BY account_type) AS account_type_sum_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior,
        LAG(balance, 1, 0) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS prev_balance,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS balance_bucket
    FROM ranked
),
metrics_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(account_type_sum_balance) OVER w_hist AS running_sum_account_type_sum_balance,
        MIN(account_type_sum_balance) OVER w_hist AS running_min_account_type_sum_balance,
        MAX(account_type_sum_balance) OVER w_hist AS running_max_account_type_sum_balance,
        AVG(balance_avg_5_prior) OVER (PARTITION BY account_type ORDER BY balance_avg_5_prior ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior_avg_5_prior
    FROM metrics
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY prev_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_accounts_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        balance,
        prev_balance,
        running_sum_account_type_sum_balance,
        running_min_account_type_sum_balance,
        balance_avg_5_prior_avg_5_prior,
        MIN(account_type_sum_balance) OVER (PARTITION BY account_type) AS account_type_min_account_type_sum_balance,
        LAG(account_type_sum_balance, 1) OVER (ORDER BY balance_avg_5_prior ASC, id ASC) AS prev_account_type_sum_balance,
        RANK() OVER (PARTITION BY account_type ORDER BY prev_balance DESC NULLS FIRST) AS prev_balance_rank
    FROM metrics_accounts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        prev_balance,
        running_sum_account_type_sum_balance,
        running_min_account_type_sum_balance,
        balance_avg_5_prior_avg_5_prior,
        account_type_min_account_type_sum_balance,
        ROUND(running_min_account_type_sum_balance, 0) AS running_min_account_type_sum_balance_rounded,
        LOWER(account_type) AS account_type_lower
    FROM metrics_accounts_2
    WHERE prev_account_type_sum_balance < 208467
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY prev_balance ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    account_type,
    metric_name,
    metric_value
FROM (
    SELECT
        id,
        account_type,
        balance,
        running_min_account_type_sum_balance,
        running_sum_account_type_sum_balance
    FROM latest
) src
UNPIVOT (
    metric_value FOR metric_name IN (balance AS BALANCE_VAL, running_min_account_type_sum_balance AS RUNNING_MIN_ACCOUNT_TYPE_SUM_BALANCE, running_sum_account_type_sum_balance AS RUNNING_SUM_ACCOUNT_TYPE_SUM_BALANCE_VAL)
) u
