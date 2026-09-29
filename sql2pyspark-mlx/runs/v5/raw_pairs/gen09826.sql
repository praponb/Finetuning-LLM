WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        SUM(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date, id) AS seq_no
    FROM eligible
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(running_balance) OVER w_ordered AS running_count_running_balance,
        MAX(running_balance) OVER w_ordered AS running_max_running_balance,
        AVG(running_balance) OVER w_ordered AS running_avg_running_balance,
        CUME_DIST() OVER w_part AS last_known_balance_cume_dist
    FROM enriched
    WHERE account_type IN ('LOAN', 'CREDIT', 'SAVINGS')
    WINDOW w_ordered AS (PARTITION BY status ORDER BY last_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status, account_type ORDER BY last_known_balance DESC NULLS LAST)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, customer_id
    FROM scored
    EXCEPT ALL
    SELECT status, customer_id
    FROM accounts
    WHERE status <> 'FROZEN'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.customer_id = k.customer_id
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    last_known_balance,
    seq_no,
    running_count_running_balance
FROM kept_rows
ORDER BY opened_date NULLS FIRST;
