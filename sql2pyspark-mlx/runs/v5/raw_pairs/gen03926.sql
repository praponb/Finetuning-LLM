WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(balance, 2) AS balance_rounded,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE status IS DISTINCT FROM 'FROZEN'
      AND customer_id = 37
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance_rounded DESC NULLS FIRST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        balance,
        status,
        resolved_status,
        RANK() OVER (PARTITION BY status ORDER BY balance_rounded DESC) AS balance_rounded_rank,
        LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY balance, id) AS next_balance
    FROM deduped
    WHERE status <> 'FROZEN'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IS NULL
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS event_seq,
        LAG(balance_rounded_rank, 1) OVER (ORDER BY balance, id) AS prev_balance_rounded_rank
    FROM not_flagged
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    CASE WHEN next_balance >= 83000 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_balance_tier,
    account_type,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS balance_per_checking
FROM enriched
GROUP BY CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_balance >= 83000 THEN 'PREMIUM' ELSE 'STANDARD' END, account_type;
