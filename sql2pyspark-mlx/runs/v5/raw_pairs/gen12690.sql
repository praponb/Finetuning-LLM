WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN monthly_fee >= 258 THEN 'High' WHEN monthly_fee >= 90 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level,
        CASE WHEN monthly_fee >= 231 THEN 'High' WHEN monthly_fee >= 85 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level_2
    FROM valid
    WHERE customer_id IS NOT NULL
      AND plan = 'premium'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_6_prior,
        LEAD(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY start_date_month_end ASC, id ASC) AS next_monthly_fee,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_monthly_fee
    FROM base
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.city,
        d.name
    FROM with_windows s
    INNER JOIN customers d
        ON s.customer_id <=> d.id
),
with_windows_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee_level,
        monthly_fee_level_2,
        customer_id_stddev_samp_monthly_fee,
        city,
        name,
        NTILE(3) OVER w_hist AS customer_id_stddev_samp_monthly_fee_bucket,
        AVG(customer_id_stddev_samp_monthly_fee) OVER (PARTITION BY plan, city ORDER BY monthly_fee_avg_6_prior ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS customer_id_stddev_samp_monthly_fee_avg_3,
        SUM(monthly_fee) OVER w_grp AS cumulative_monthly_fee,
        FIRST_VALUE(monthly_fee_avg_6_prior, TRUE) OVER (PARTITION BY customer_id, city ORDER BY start_date_month_end ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_avg_6_prior
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY city, plan ORDER BY customer_id_stddev_samp_monthly_fee DESC, id ASC), w_grp AS (PARTITION BY plan ORDER BY next_monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.monthly_fee_level,
    s.monthly_fee_level_2,
    s.customer_id_stddev_samp_monthly_fee,
    s.name,
    s.customer_id_stddev_samp_monthly_fee_bucket,
    s.customer_id_stddev_samp_monthly_fee_avg_3,
    CASE WHEN name RLIKE '^(Grace Kim|Henry Ford)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS name_zone,
    (SELECT MAX(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM with_windows_subscriptions s
ORDER BY customer_id_stddev_samp_monthly_fee DESC NULLS LAST, plan DESC;
