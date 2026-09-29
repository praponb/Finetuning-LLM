WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.balance,
        e.opened_date,
        p.email,
        p.name,
        p.country
    FROM kept e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(balance) OVER (PARTITION BY country ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_balance,
        AVG(balance) OVER (PARTITION BY country ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_balance,
        COUNT(balance) OVER (PARTITION BY country ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_balance
    FROM with_dims
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.name,
        s.country,
        s.running_max_balance,
        x.amount
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        country,
        amount,
        running_max_balance
    FROM with_last
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric_name IN (amount, running_max_balance)
)
