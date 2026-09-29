WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        COALESCE(segment, 'OTHER') AS segment_filled
    FROM customers
    WHERE signup_date >= DATE '2023-02-08'
      AND city IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(days_since_signup_date) OVER (PARTITION BY city, country ORDER BY days_since_signup_date, id) AS next_days_since_signup_date,
        LAG(credit_limit) OVER (PARTITION BY city, country ORDER BY days_since_signup_date, id) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY days_since_signup_date, id) AS row_seq,
        AVG(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS age_avg_3_prior
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(next_days_since_signup_date) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next_days_since_signup_date_avg_2_prior,
        AVG(age_avg_3_prior) OVER (PARTITION BY country ORDER BY next_days_since_signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_3_prior_avg_5,
        STDDEV_SAMP(age_avg_3_prior) OVER (PARTITION BY city, segment) AS city_stddev_samp_age_avg_3_prior,
        AVG(next_days_since_signup_date) OVER (PARTITION BY country ORDER BY days_since_signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_days_since_signup_date_avg_5,
        SUM(next_days_since_signup_date) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_days_since_signup_date
    FROM with_windows
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.is_active,
        s.credit_limit,
        s.age,
        s.days_since_signup_date,
        s.next_days_since_signup_date,
        s.prev_credit_limit,
        s.next_days_since_signup_date_avg_2_prior,
        s.age_avg_3_prior_avg_5,
        s.cumulative_next_days_since_signup_date,
        x.channel,
        x.status
    FROM metrics s
    JOIN LATERAL (
            SELECT c.channel, c.status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cumulative_next_days_since_signup_date) OVER (PARTITION BY status, channel ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_cumulative_next_days_since_signup_date,
        AVG(cumulative_next_days_since_signup_date) OVER (PARTITION BY status, channel ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_cumulative_next_days_since_signup_date,
        MIN(cumulative_next_days_since_signup_date) OVER (PARTITION BY status, channel ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_cumulative_next_days_since_signup_date,
        COUNT(cumulative_next_days_since_signup_date) OVER (PARTITION BY status, channel ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_cumulative_next_days_since_signup_date
    FROM with_last
    WHERE next_days_since_signup_date != 67
)
SELECT
    id,
    country,
    is_active,
    credit_limit,
    age,
    days_since_signup_date,
    next_days_since_signup_date,
    prev_credit_limit,
    next_days_since_signup_date_avg_2_prior,
    CASE WHEN country RLIKE '^(SG|JP)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'CORE' ELSE 'GROUP_A' END AS country_zone
FROM metrics_customers
ORDER BY country_zone DESC;
