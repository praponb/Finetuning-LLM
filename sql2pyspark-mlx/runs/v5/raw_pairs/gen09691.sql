WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        ROUND(balance * 100, 2) AS adjusted_balance
    FROM accounts
    WHERE opened_date >= DATE '2023-03-25'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY adjusted_balance DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        status,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_2,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS row_seq,
        LAST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status
    FROM deduped
),
deduped_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY row_seq DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        status,
        first_known_status,
        first_known_status_2,
        row_seq,
        last_known_status,
        LEAD(row_seq, 2) OVER (PARTITION BY account_type ORDER BY row_seq, id) AS next_row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY row_seq DESC, id) AS row_seq_2,
        SUM(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY status, account_type), 4) AS row_seq_share,
        RANK() OVER w_hist AS row_seq_rank
    FROM deduped_accounts
    WINDOW w_hist AS (ORDER BY row_seq DESC NULLS LAST)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        status,
        first_known_status,
        row_seq,
        row_seq_2,
        row_seq_share,
        row_seq_rank
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY row_seq_share, id) = 1
),
with_windows_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        first_known_status,
        row_seq,
        row_seq_rank,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY status), 4) AS row_seq_share_2,
        LAST_VALUE(row_seq_rank, TRUE) OVER w_grp AS last_known_row_seq_rank,
        AVG(row_seq) OVER w_recent AS row_seq_avg_6_prior,
        ROUND(row_seq_2 / SUM(row_seq_2) OVER (PARTITION BY status), 4) AS row_seq_2_share
    FROM latest
    WINDOW w_grp AS (PARTITION BY status ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY row_seq_share, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq_2_share >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_2_share_tier,
    CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    MAX(last_known_row_seq_rank) AS max_last_known_row_seq_rank
FROM with_windows_accounts
GROUP BY GROUPING SETS ((CASE WHEN row_seq_2_share >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN row_seq_2_share >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY avg_row_seq
