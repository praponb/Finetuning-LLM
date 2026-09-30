WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        CEIL(balance) balance_whole
    FROM accounts
    WHERE opened_date >= DATE '2023-06-06'
      AND status IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_hist balance_whole_dense_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY balance, id) balance_bucket
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY status, account_type ORDER BY balance_whole NULLS FIRST)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_type, status ORDER BY balance_whole DESC NULLS FIRST) AS dedup_rank
        FROM with_windows
    ) ranked
    WHERE dedup_rank = 1
),
with_windows_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        balance_whole,
        balance_whole_dense_rank,
        balance_bucket,
        dedup_rank,
        SUM(balance_whole_dense_rank) OVER (PARTITION BY status ORDER BY balance_whole_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance_whole_dense_rank,
        FIRST_VALUE(balance_bucket, TRUE) OVER (ORDER BY balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY dedup_rank, id) seq_no
    FROM latest
)
-- Grouped metrics
SELECT
    CASE WHEN balance >= 80000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    seq_no,
    SIZE(COLLECT_SET(status)) AS num_status,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    PERCENTILE_APPROX(balance_whole, 0.9) AS balance_whole_p90
FROM with_windows_accounts
GROUP BY CASE WHEN balance >= 80000 THEN 'LARGE' ELSE 'SMALL' END, seq_no
HAVING num_status >= 1
ORDER BY seq_no DESC NULLS FIRST;
