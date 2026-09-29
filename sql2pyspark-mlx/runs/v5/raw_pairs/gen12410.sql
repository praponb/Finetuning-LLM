WITH with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_merchant,
        x.transaction_txn_type
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_txn_type ORDER BY opened_date DESC, id ASC) AS row_num
        FROM with_last
    ) t
    WHERE row_num = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        c.age,
        c.name
    FROM ranked src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        age,
        name,
        MIN(age) OVER (PARTITION BY customer_id ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age,
        COUNT(age) OVER (PARTITION BY customer_id ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        AVG(age) OVER (PARTITION BY customer_id ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age
    FROM joined
    WHERE customer_id IS NOT NULL
),
with_last_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.age,
        s.running_min_age,
        s.running_count_age,
        s.running_avg_age,
        x.transaction_amount,
        x.transaction_txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Join a dimension and aggregate
SELECT
    d.country,
    a.transaction_txn_type,
    COUNT(*) AS cnt,
    AVG(a.running_min_age) AS avg_running_min_age,
    GROUPING_ID() AS grouping_level
FROM with_last_accounts a
JOIN customers d ON a.customer_id = d.id
GROUP BY GROUPING SETS ((d.country, a.transaction_txn_type), (d.country), ())
