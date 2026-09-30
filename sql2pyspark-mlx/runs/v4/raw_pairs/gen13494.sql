WITH base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(txn_type) AS txn_type_length
    FROM transactions
    WHERE txn_time > TO_DATE('2023-12-21')
      AND merchant IS NOT NULL
      AND merchant = 'Lazada'
),
enriched AS (
    -- Attach accounts attributes
    SELECT src.*, a.account_type
    FROM base src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        d.account_type AS account_account_type,
        d.opened_date
    FROM enriched s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY opened_date, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.balance,
        a.status
    FROM ranked s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN amount >= 4400 THEN 'High' WHEN amount >= 900 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM enriched_transactions
    WHERE account_type = 'CHECKING'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT txn_type, id
    FROM valid
    EXCEPT ALL
    SELECT txn_type, id
    FROM transactions
    WHERE merchant NOT IN ('Starbucks', 'Amazon')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN txn_type_length >= 85 THEN 'LARGE' ELSE 'SMALL' END AS txn_type_length_tier,
    COUNT(DISTINCT account_type) AS distinct_account_types,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    COUNT(CASE WHEN account_account_type = 'LOAN' THEN 1 END) AS loan_count,
    STDDEV_SAMP(amount) AS amount_stddev,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', opened_date) ORDER BY SIZE(COLLECT_SET(merchant)) DESC) AS num_merchants_rank
FROM kept_rows
GROUP BY DATE_TRUNC('QUARTER', opened_date), CASE WHEN txn_type_length >= 85 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY amount_stddev DESC
