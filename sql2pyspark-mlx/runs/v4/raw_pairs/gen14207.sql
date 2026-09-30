WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged = TRUE
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date NULLS FIRST, id) AS latest_rank
        FROM kept
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance, 1) OVER (ORDER BY opened_date, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY latest_rank DESC, id) AS seq_no
    FROM ranked
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        AVG(seq_no) AS avg_seq_no,
        ROUND(SUM(prev_balance) / NULLIF(COUNT(CASE WHEN status = 'FROZEN' THEN 1 END), 0), 2) AS prev_balance_per_frozen,
        GROUPING(seq_no) AS grouping_id
    FROM eligible
    GROUP BY ROLLUP (seq_no)
    HAVING ROUND(SUM(prev_balance) / NULLIF(COUNT(CASE WHEN status = 'FROZEN' THEN 1 END), 0), 2) > 0
)
SELECT
    seq_no,
    avg_seq_no,
    grouping_id
FROM stats
ORDER BY grouping_id DESC
