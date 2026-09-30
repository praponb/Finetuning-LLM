WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LOWER(account_type) AS account_type_lower,
        COALESCE(status, 'OTHER') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-08-20'
      AND customer_id IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount < 2000
        )
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM not_flagged src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL 7 DAYS
),
eligible AS (
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
valid_accounts AS (
    SELECT *, TRIM(status) AS clean_status
    FROM eligible
    WHERE opened_date >= TO_DATE('2023-09-28')
      AND customer_id = 18
),
kept AS (
    SELECT s.*
    FROM valid_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
)
SELECT
    subscription_id,
    CASE WHEN subscription_id >= 10 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SUM(CASE WHEN account_type = 'CREDIT' THEN subscription_id ELSE 0 END) AS credit_subscription_id,
    MAX(monthly_fee) FILTER (WHERE plan = 'premium') AS premium_max_monthly_fee,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_per_enterprise,
    COUNT_IF(plan = 'basic') AS basic_events,
    GROUPING_ID() AS grouping_level
FROM kept
GROUP BY ROLLUP (subscription_id, CASE WHEN subscription_id >= 10 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', opened_date))
