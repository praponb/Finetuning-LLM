WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        balance,
        opened_date,
        status,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(balance, 0) AS balance_filled
    FROM accounts
    WHERE status IS NOT NULL
      AND balance BETWEEN 44000 AND 64000
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        opened_date,
        status,
        status_title,
        balance_filled,
        LAST_DAY(opened_date) AS opened_date_month_end,
        CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END AS is_closed
    FROM cleaned
    WHERE opened_date >= DATE '2023-10-14'
      AND status IS NOT NULL
      AND balance_filled >= 45
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND NOT c.is_flagged
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_closed) OVER w_part AS is_closed_avg_5_prior,
        NTILE(10) OVER w_running AS balance_filled_bucket,
        LEAD(balance_filled, 1) OVER (PARTITION BY status ORDER BY is_closed, id) AS next_balance_filled
    FROM kept
    WINDOW w_part AS (PARTITION BY status ORDER BY balance_filled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY status ORDER BY balance_filled, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY next_balance_filled DESC NULLS LAST, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    opened_date,
    status_title,
    balance_filled,
    opened_date_month_end,
    is_closed,
    is_closed_avg_5_prior,
    balance_filled_bucket,
    next_balance_filled
FROM ranked
ORDER BY balance_filled_bucket DESC NULLS LAST
