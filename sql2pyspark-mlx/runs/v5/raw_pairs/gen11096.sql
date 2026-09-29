WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER w_grp AS running_avg_balance,
        MIN(balance) OVER w_grp AS running_min_balance,
        MAX(balance) OVER w_grp AS running_max_balance,
        SUM(balance) OVER w_grp AS running_sum_balance,
        DENSE_RANK() OVER w_part AS balance_dense_rank
    FROM accounts
    WINDOW w_grp AS (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY balance NULLS FIRST)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.name,
        p.country
    FROM with_windows e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_type,
        x.transaction_amount
    FROM enriched s
    JOIN LATERAL (
            SELECT c.txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        running_min_balance,
        running_max_balance,
        balance_dense_rank,
        name,
        txn_type
    FROM with_last
    QUALIFY RANK() OVER (PARTITION BY account_type ORDER BY balance_dense_rank) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (1, 11)
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city,
        d.name AS customer_name,
        d.email
    FROM kept a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE LENGTH(d.name) < 12
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.running_max_balance,
        s.balance_dense_rank,
        s.txn_type,
        s.city,
        d.email AS customer_email,
        d.segment,
        d.name AS customer_name_2
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
summary AS (
    -- Grouped metrics
    SELECT
        txn_type,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
        ROUND(SUM(balance_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS balance_dense_rank_per_bangkok,
        GROUPING(txn_type) AS gid
    FROM enriched_accounts
    GROUP BY ROLLUP (txn_type)
    HAVING ROUND(SUM(balance_dense_rank) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) > 0
)
SELECT txn_type, balance_dense_rank_per_bangkok
FROM summary;
