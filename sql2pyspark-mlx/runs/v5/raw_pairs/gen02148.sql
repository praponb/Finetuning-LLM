WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS dedup_rank
        FROM accounts
    ) ranked
    WHERE dedup_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM latest
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE MONTH(opened_date) IN (1, 2, 9)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_percent_rank,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS balance_dense_rank
    FROM kept_rows
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, b.segment
    FROM scored e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    ROUND(AVG(balance_dense_rank), 2) AS avg_balance_dense_rank
FROM enriched
GROUP BY customer_id, DATE_TRUNC('QUARTER', opened_date)
HAVING distinct_account_types >= 1
