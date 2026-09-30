WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        INITCAP(TRIM(browser)) browser_title,
        LENGTH(device) device_length
    FROM sessions
    WHERE started_at > DATE '2023-07-10'
      AND browser IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered e
    JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY browser ORDER BY monthly_fee DESC) AS row_num
    FROM with_window
    QUALIFY row_num = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        browser_title,
        device_length,
        subscription_id,
        plan,
        monthly_fee,
        row_num,
        SUM(monthly_fee) OVER (PARTITION BY device, country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) monthly_fee_sum_90d,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
        NTILE(3) OVER (PARTITION BY device ORDER BY subscription_id DESC, id, subscription_id) subscription_id_bucket
    FROM latest
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN subscription_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS subscription_id_tier,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        SUM(CASE WHEN plan = 'standard' THEN subscription_id_bucket ELSE 0 END) AS standard_subscription_id_bucket,
        MAX(started_at) AS ended_at
    FROM scored
    GROUP BY subscription_id_tier
)
SELECT
    subscription_id_tier,
    plan_list,
    standard_subscription_id_bucket
FROM grouped
ORDER BY subscription_id_tier;
