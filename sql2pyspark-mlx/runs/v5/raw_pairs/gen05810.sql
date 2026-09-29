WITH joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.balance,
        a.opened_date,
        a.status,
        p.signup_date,
        p.name
    FROM accounts a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.name LIKE 'C%'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        opened_date,
        status,
        signup_date,
        name,
        ROUND(balance * 0.9, 2) AS adjusted_balance,
        COALESCE(status, 'OTHER') AS status_clean
    FROM joined
    WHERE status != 'FROZEN'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(adjusted_balance) OVER w_recent AS prev_adjusted_balance,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM base
    WINDOW w_recent AS (PARTITION BY status ORDER BY opened_date, id)
),
enriched_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(adjusted_balance) OVER w_hist AS running_min_adjusted_balance,
        MAX(adjusted_balance) OVER w_hist AS running_max_adjusted_balance,
        COUNT(adjusted_balance) OVER w_hist AS running_count_adjusted_balance,
        SUM(adjusted_balance) OVER w_hist AS running_sum_adjusted_balance,
        COUNT(adjusted_balance) OVER (PARTITION BY status) AS status_count_adjusted_balance
    FROM enriched
    WINDOW w_hist AS (PARTITION BY status ORDER BY prev_adjusted_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.txn_type,
        latest.transaction_merchant
    FROM enriched_accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_type, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN adjusted_balance >= 76 THEN 'HIGH' ELSE 'LOW' END AS adjusted_balance_tier,
        COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
        ROUND(AVG(running_min_adjusted_balance), 2) AS avg_running_min_adjusted_balance,
        COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows
    FROM with_latest
    GROUP BY adjusted_balance_tier
)
SELECT
    adjusted_balance_tier,
    closed_count,
    avg_running_min_adjusted_balance
FROM grouped
ORDER BY avg_running_min_adjusted_balance DESC NULLS FIRST;
