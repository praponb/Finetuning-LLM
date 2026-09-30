WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(segment) AS segment_upper,
        WEEKOFYEAR(signup_date) AS signup_date_weekofyear,
        COALESCE(city, 'N/A') AS city_filled
    FROM customers
    WHERE signup_date > DATE '2023-03-06'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(signup_date_weekofyear) OVER (ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS signup_date_weekofyear_avg_3,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS event_seq
    FROM valid
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.subscription_end_date
    FROM enriched s
    JOIN LATERAL (
            SELECT c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 13200 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    CASE WHEN age >= 68 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    AVG(credit_limit) FILTER (WHERE country = 'US') AS us_avg_credit_limit,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN city = 'London' THEN age ELSE 0 END) AS london_age,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS event_seq_per_sg,
    COUNT_IF(segment = 'Corporate') AS corporate_events
FROM latest_match
GROUP BY credit_limit_tier, age_tier;
