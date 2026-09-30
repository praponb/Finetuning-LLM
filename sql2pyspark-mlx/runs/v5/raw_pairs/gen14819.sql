WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date >= sub.start_date
       AND s.start_date < sub.end_date + INTERVAL '7' DAY
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        subscription_id
    FROM matched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, subscription_plan ORDER BY monthly_fee DESC NULLS FIRST, id, subscription_id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(subscription_id) OVER (PARTITION BY plan ORDER BY end_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_id,
        AVG(subscription_id) OVER w_recent AS subscription_id_avg_3
    FROM ranked
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY start_date, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY cumulative_subscription_id NULLS LAST, id, subscription_id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, row_num - subscription_id AS row_num_minus_subscription_id
    FROM deduped
    WHERE subscription_id <= 5
),
scored_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(cumulative_subscription_id) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id_avg_3, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_cumulative_subscription_id,
        AVG(cumulative_subscription_id) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id_avg_3, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_cumulative_subscription_id,
        SUM(cumulative_subscription_id) OVER (PARTITION BY customer_id, plan ORDER BY subscription_id_avg_3, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_cumulative_subscription_id,
        SUM(row_num_minus_subscription_id) OVER (PARTITION BY customer_id ORDER BY start_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_num_minus_subscription_id
    FROM base
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, last4_avg_cumulative_subscription_id + running_row_num_minus_subscription_id AS last4_avg_cumulative_subscription_id_plus_running_row_num_minus_subscription_id
    FROM scored_subscriptions
    WHERE row_num >= 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_row_num_minus_subscription_id >= 309 THEN 'HIGH' ELSE 'LOW' END AS running_row_num_minus_subscription_id_tier,
        ROUND(AVG(subscription_id), 2) AS avg_subscription_id,
        APPROX_PERCENTILE(cumulative_subscription_id, 0.95) AS cumulative_subscription_id_p95,
        COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
        PERCENTILE_APPROX(cumulative_subscription_id, ARRAY(0.25, 0.5, 0.75)) AS cumulative_subscription_id_quartiles,
        ROUND(SUM(last4_min_cumulative_subscription_id) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS last4_min_cumulative_subscription_id_per_basic
    FROM base_subscriptions
    GROUP BY running_row_num_minus_subscription_id_tier
)
SELECT
    running_row_num_minus_subscription_id_tier,
    avg_subscription_id,
    cumulative_subscription_id_p95,
    last4_min_cumulative_subscription_id_per_basic
FROM stats
ORDER BY avg_subscription_id DESC NULLS LAST;
