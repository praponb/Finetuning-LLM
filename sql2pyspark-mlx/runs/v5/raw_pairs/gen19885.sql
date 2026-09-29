WITH with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.age,
        x.balance
    FROM customers s
    JOIN LATERAL (
            SELECT c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(balance) OVER w_grp AS prev_balance,
        LEAD(balance) OVER w_grp AS next_balance,
        LEAD(age) OVER w_grp AS next_age,
        ROW_NUMBER() OVER w_grp AS event_seq,
        AVG(age) OVER (PARTITION BY city ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS age_avg_3
    FROM with_latest
    WHERE city IS NOT NULL
    WINDOW w_grp AS (PARTITION BY country ORDER BY age ASC, id ASC)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.balance <= 59000
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_balance) OVER w_part AS last4_sum_prev_balance,
        MAX(prev_balance) OVER w_part AS last4_max_prev_balance,
        AVG(prev_balance) OVER w_part AS last4_avg_prev_balance,
        COUNT(prev_balance) OVER w_part AS last4_count_prev_balance,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY balance DESC, id ASC) AS row_seq
    FROM kept
    WINDOW w_part AS (PARTITION BY country ORDER BY next_age ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
kept_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date < s.signup_date
        )
),
scored_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        signup_date,
        is_active,
        balance,
        next_balance,
        next_age,
        event_seq,
        age_avg_3,
        last4_sum_prev_balance,
        last4_avg_prev_balance,
        ROUND(next_age / SUM(next_age) OVER (PARTITION BY country), 4) AS next_age_share,
        LEAD(event_seq, 1, 0) OVER (PARTITION BY country ORDER BY last4_sum_prev_balance ASC, id ASC) AS next_event_seq
    FROM kept_customers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM scored_customers
    WHERE next_age_share < 0.41
)
SELECT
    id,
    email,
    signup_date,
    is_active,
    balance,
    event_seq,
    age_avg_3,
    last4_sum_prev_balance,
    last4_avg_prev_balance
FROM valid
ORDER BY balance ASC NULLS LAST
