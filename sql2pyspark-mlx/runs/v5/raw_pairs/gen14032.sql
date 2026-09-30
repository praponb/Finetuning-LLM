WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        TO_DATE(txn_time) txn_time_date,
        LOWER(txn_type) txn_type_lower
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= '2024-01-01'
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.merchant,
        s.txn_time_date,
        a.account_type,
        a.status
    FROM base s
    JOIN accounts a
        ON s.account_id = a.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        account_type,
        FIRST_VALUE(account_type, TRUE) OVER (ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_account_type,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_hist seq_no,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) amount_avg_6_prior,
        LAG(amount, 1, 0) OVER (PARTITION BY txn_type ORDER BY amount, id) prev_amount
    FROM with_windows
    WINDOW w_hist AS (ORDER BY amount DESC, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY amount_avg_6_prior DESC, id) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
summary AS (
    -- Multi-level aggregation
    SELECT
        account_type,
        CASE WHEN prev_amount >= 3600 THEN 'HIGH' ELSE 'LOW' END AS prev_amount_tier,
        COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count,
        SUM(CASE WHEN txn_type = 'DEBIT' THEN amount_avg_6_prior ELSE 0 END) AS debit_amount_avg_6_prior,
        COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows,
        COUNT_IF(merchant = '7-Eleven') AS 7_eleven_events
    FROM ranked
    GROUP BY account_type, CASE WHEN prev_amount >= 3600 THEN 'HIGH' ELSE 'LOW' END
    HAVING COUNT(*) FILTER (WHERE txn_type = 'REFUND') > 1
)
SELECT
    account_type,
    prev_amount_tier,
    row_count,
    shell_count,
    refund_rows
FROM summary
ORDER BY prev_amount_tier NULLS LAST
