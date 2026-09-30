WITH latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.city,
        s.segment,
        s.signup_date,
        s.credit_limit,
        s.age,
        latest.monthly_fee
    FROM customers s
    JOIN LATERAL (
            SELECT c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.customer_id IS NOT NULL
        )
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.signup_date,
        s.credit_limit,
        s.age,
        s.monthly_fee,
        x.status
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        credit_limit,
        AVG(credit_limit) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_6_prior,
        CUME_DIST() OVER w_ordered AS monthly_fee_cume_dist,
        LAG(credit_limit, 2) OVER (PARTITION BY status, city ORDER BY signup_date, id) AS prev_credit_limit
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY status ORDER BY monthly_fee DESC)
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IS NULL
        )
)
-- Aggregate with conditional metrics
SELECT
    city,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows_2,
    PERCENTILE_APPROX(credit_limit_avg_6_prior, 0.9) AS credit_limit_avg_6_prior_p90,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(AVG(prev_credit_limit), 2) AS avg_prev_credit_limit
FROM not_flagged_customers
GROUP BY city;
