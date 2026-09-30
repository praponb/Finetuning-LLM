WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(status) AS status_length
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'CLOSED'
      AND balance > 37000
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(status_length) OVER w_grp AS running_sum_status_length,
        MIN(status_length) OVER w_grp AS running_min_status_length,
        COUNT(status_length) OVER w_grp AS running_count_status_length,
        NTILE(10) OVER w_hist AS status_length_bucket
    FROM base
    WHERE customer_id IS NULL
    WINDOW w_grp AS (PARTITION BY account_type ORDER BY status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY customer_id ORDER BY status_length DESC, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY running_count_status_length, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
scored_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        status_length,
        running_sum_status_length,
        status_length_bucket,
        SUM(status_length) OVER (PARTITION BY account_type ORDER BY status_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_status_length,
        AVG(status_length) OVER (PARTITION BY account_type ORDER BY status_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_status_length,
        MIN(status_length) OVER (PARTITION BY account_type ORDER BY status_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_status_length
    FROM kept
),
report AS (
    SELECT
        id,
        opened_date,
        status_length,
        running_sum_status_length,
        status_length_bucket,
        last7_avg_status_length,
        last7_min_status_length
    FROM scored_accounts
    ORDER BY id DESC NULLS FIRST
)
SELECT
    id,
    opened_date,
    last7_min_status_length
FROM report;
