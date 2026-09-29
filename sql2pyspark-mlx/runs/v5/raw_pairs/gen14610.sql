WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        AVG(balance) OVER (ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) balance_avg_5_prior,
        NTILE(4) OVER (ORDER BY balance DESC, id) balance_bucket,
        NTILE(4) OVER (PARTITION BY account_type, status ORDER BY balance DESC, id) balance_bucket_2,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) row_seq
    FROM accounts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.opened_date,
        src.status,
        src.balance_bucket,
        src.row_seq,
        d.name,
        d.country
    FROM with_windows src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(balance_bucket) OVER (PARTITION BY country, customer_id ORDER BY opened_date, id) next_balance_bucket,
        LEAD(row_seq) OVER (PARTITION BY country, customer_id ORDER BY opened_date, id) next_row_seq,
        LAG(row_seq) OVER (PARTITION BY country, customer_id ORDER BY opened_date, id) prev_row_seq,
        ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY opened_date, id) seq_no
    FROM with_dims
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type NOT IN ('DEBIT', 'FEE')
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM kept
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY row_seq DESC NULLS LAST, id) = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    COUNT(DISTINCT status) AS distinct_status,
    APPROX_PERCENTILE(transaction_amount, ARRAY(0.25, 0.5, 0.75)) AS transaction_amount_quartiles
FROM with_latest
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', opened_date), DATE_TRUNC('YEAR', opened_date)), (DATE_TRUNC('WEEK', opened_date)), ());
