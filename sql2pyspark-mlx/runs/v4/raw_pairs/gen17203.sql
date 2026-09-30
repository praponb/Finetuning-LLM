WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, c.country
    FROM not_flagged a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date BETWEEN sub.start_date AND sub.end_date
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY monthly_fee DESC, id, subscription_id) AS row_seq,
        AVG(subscription_id) OVER (PARTITION BY customer_id, status ORDER BY opened_date, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS subscription_id_avg_6,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY status, plan ORDER BY opened_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        SUM(subscription_id) OVER (PARTITION BY customer_id) AS customer_id_sum_subscription_id,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM attributed
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id, status
    FROM accounts
    WHERE account_type = 'CREDIT'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        account_type,
        balance,
        status,
        subscription_id,
        plan,
        monthly_fee,
        row_seq,
        subscription_id_avg_6,
        customer_id_sum_subscription_id,
        last_known_country,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM verified
    WHERE opened_date > DATE '2023-12-04'
      AND customer_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    customer_id_sum_subscription_id,
    PERCENTILE_APPROX(days_since_opened_date, 0.9) AS days_since_opened_date_p90,
    COUNT_IF(account_type = 'LOAN') AS loan_events,
    COUNT_IF(status = 'FROZEN') AS frozen_events
FROM filtered
GROUP BY row_seq_tier, customer_id_sum_subscription_id;
