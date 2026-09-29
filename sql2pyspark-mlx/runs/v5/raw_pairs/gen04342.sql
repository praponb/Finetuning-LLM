WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(opened_date, 46) opened_date_minus_46d
    FROM accounts
    WHERE status IN ('CLOSED', 'OPEN')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date_minus_46d >= sub.start_date
       AND e.opened_date_minus_46d < sub.end_date + INTERVAL 14 DAYS
),
cleaned_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('YEAR', opened_date_minus_46d) opened_date_minus_46d_period
    FROM with_window
    WHERE opened_date_minus_46d BETWEEN '2024-03-29' AND '2025-03-12'
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.status,
        s.opened_date_minus_46d,
        s.subscription_id,
        s.plan,
        s.opened_date_minus_46d_period,
        latest.txn_time
    FROM cleaned_accounts s
    JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY subscription_id ASC NULLS FIRST) subscription_id_percent_rank,
        NTILE(10) OVER (ORDER BY subscription_id ASC, id ASC, subscription_id ASC) subscription_id_bucket
    FROM with_last
    WHERE balance BETWEEN 20000 AND 51000
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        opened_date_minus_46d,
        subscription_id,
        plan,
        opened_date_minus_46d_period,
        txn_time,
        FIRST_VALUE(status, TRUE) OVER (ORDER BY balance ASC, id ASC, subscription_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status,
        LEAD(subscription_id, 1, 0) OVER w_hist next_subscription_id
    FROM scored
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY subscription_id_percent_rank ASC, id ASC, subscription_id ASC)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM enriched
    INTERSECT ALL
    SELECT id, status
    FROM accounts
    WHERE customer_id = 10
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    plan,
    DATE_TRUNC('WEEK', opened_date_minus_46d_period) AS week_start,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(AVG(subscription_id), 2) AS avg_subscription_id,
    MIN(subscription_id) AS min_subscription_id,
    ROUND(SUM(subscription_id) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS subscription_id_per_enterprise,
    MAX(subscription_id) AS max_subscription_id,
    ROUND(AVG(next_subscription_id), 2) AS avg_next_subscription_id,
    GROUPING_ID() AS grouping_id
FROM verified
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', txn_time), plan, DATE_TRUNC('WEEK', opened_date_minus_46d_period)), (DATE_TRUNC('YEAR', txn_time), plan));
