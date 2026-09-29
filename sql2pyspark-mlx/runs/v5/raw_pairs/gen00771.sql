WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
    FROM accounts
    WHERE account_type = 'LOAN'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, ROW_NUMBER() OVER (ORDER BY opened_date DESC, id ASC) AS row_seq
    FROM base
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY row_seq DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC, id ASC) AS seq_no,
        AVG(row_seq) OVER (ORDER BY row_seq ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_seq_avg_5,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY customer_id), 4) AS row_seq_share,
        LEAD(row_seq, 2) OVER (PARTITION BY status ORDER BY opened_date ASC, id ASC) AS next_row_seq
    FROM not_flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date ASC NULLS FIRST, id ASC) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
with_windows_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(seq_no) OVER (PARTITION BY customer_id ORDER BY seq_no ASC, id ASC) AS next_seq_no,
        LAG(seq_no) OVER (PARTITION BY customer_id ORDER BY seq_no ASC, id ASC) AS prev_seq_no,
        LEAD(row_seq_share) OVER (PARTITION BY customer_id ORDER BY seq_no ASC, id ASC) AS next_row_seq_share,
        LAG(row_seq_share) OVER (PARTITION BY customer_id ORDER BY seq_no ASC, id ASC) AS prev_row_seq_share,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY seq_no ASC, id ASC) AS row_seq_2,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY event_seq ASC NULLS LAST) AS event_seq_dense_rank
    FROM ranked
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq_share >= 0.82 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_share_tier,
    MAX(prev_row_seq_share) FILTER (WHERE status = 'CLOSED') AS closed_max_prev_row_seq_share,
    COUNT(*) AS row_count,
    COUNT_IF(status = 'OPEN') AS open_events,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows_accounts
GROUP BY 1
ORDER BY row_count ASC NULLS FIRST;
