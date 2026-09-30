WITH valid AS (
    SELECT
        id,
        account_type,
        balance,
        status,
        CASE WHEN balance >= 64000 THEN 'High' WHEN balance >= 21000 THEN 'Medium' ELSE 'Low' END AS balance_tier,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-03-14'
      AND status IS NOT NULL
),
with_last AS (
    SELECT
        s.id,
        s.account_type,
        s.status,
        s.status_filled,
        x.txn_time,
        x.amount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY txn_time DESC, id) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
kept AS (
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged = TRUE
        )
)
SELECT
    id,
    account_type,
    status,
    txn_time
FROM kept
