WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC) AS prev_monthly_fee,
        LAG(monthly_fee, 2, 0) OVER (ORDER BY monthly_fee ASC, id ASC) AS prev_monthly_fee_2
    FROM subscriptions
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT s.*, d.age
    FROM enriched s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        prev_monthly_fee,
        prev_monthly_fee_2,
        age,
        LEAST(age, monthly_fee) AS least_age_monthly_fee
    FROM enriched_subscriptions
    WHERE prev_monthly_fee_2 > 190
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(prev_monthly_fee, TRUE) OVER (PARTITION BY plan, customer_id ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_monthly_fee,
        LEAD(prev_monthly_fee_2, 1) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee ASC, id ASC) AS next_prev_monthly_fee_2,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY age DESC, id ASC) AS age_bucket,
        SUM(prev_monthly_fee_2) OVER (PARTITION BY plan ORDER BY age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_monthly_fee_2,
        AVG(prev_monthly_fee_2) OVER (PARTITION BY plan ORDER BY end_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS prev_monthly_fee_2_avg_5
    FROM valid
)
SELECT
    s.id,
    s.customer_id,
    s.end_date,
    s.prev_monthly_fee,
    s.age,
    s.least_age_monthly_fee,
    s.last_known_prev_monthly_fee,
    s.next_prev_monthly_fee_2,
    s.age_bucket,
    (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM metrics s
ORDER BY least_age_monthly_fee DESC NULLS FIRST;
