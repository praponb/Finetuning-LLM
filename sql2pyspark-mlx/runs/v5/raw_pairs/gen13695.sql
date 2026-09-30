WITH enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.balance,
        e.status,
        d.email,
        d.city,
        d.credit_limit
    FROM accounts e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY customer_id ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_balance,
        MAX(balance) OVER (PARTITION BY customer_id ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_balance,
        COUNT(balance) OVER (PARTITION BY customer_id ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_balance,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance
    FROM enriched
    WHERE email LIKE '%@company.co'
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_type,
        x.merchant
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.txn_type, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(last3_max_balance) OVER w_hist AS next_last3_max_balance,
        LAG(last3_max_balance) OVER w_hist AS prev_last3_max_balance,
        LAG(balance) OVER w_hist AS prev_balance,
        ROW_NUMBER() OVER w_hist AS seq_no,
        MAX(first_known_balance) OVER w_hist_2 AS customer_id_max_first_known_balance
    FROM with_latest
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY last3_min_balance, id), w_hist_2 AS (PARTITION BY customer_id)
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    s.txn_type,
    COUNT(*) AS rows_total,
    ROUND(AVG(s.seq_no), 2) AS avg_seq_no,
    ROUND(AVG(s.last3_min_balance), 2) AS avg_last3_min_balance
FROM scored_accounts s
LEFT JOIN customers dim ON s.customer_id = dim.id
GROUP BY dim.segment, s.txn_type
ORDER BY txn_type
