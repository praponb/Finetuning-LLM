WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND account_type = 'SAVINGS'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.email,
        b.city
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT s.*, c.signup_date
    FROM enriched s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_txn_type
    FROM enriched_accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN balance >= 87000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
        APPROX_PERCENTILE(balance, 0.95) AS balance_p95,
        COUNT(*) AS cnt
    FROM with_last
    GROUP BY CASE WHEN balance >= 87000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT balance_tier, balance_p95
FROM totals
ORDER BY balance_p95 DESC NULLS FIRST, balance_tier;
