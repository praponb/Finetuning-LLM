WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DAYOFWEEK(start_date) start_date_dayofweek
    FROM subscriptions
    WHERE end_date > DATE '2023-11-07'
),
enriched AS (
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        NTILE(10) OVER (PARTITION BY status ORDER BY monthly_fee ASC, id ASC) monthly_fee_bucket,
        COUNT(start_date_dayofweek) OVER (PARTITION BY plan) plan_count_start_date_dayofweek,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY status) status_stddev_samp_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) monthly_fee_avg_2_prior
    FROM cleaned
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_bucket ASC NULLS LAST) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
valid AS (
    SELECT
        *,
        CASE WHEN status IN ('PAUSED', 'EXPIRED') THEN 'Tier 1' ELSE 'Other' END status_group,
        running_monthly_fee + plan_count_start_date_dayofweek running_monthly_fee_plus_plan_count_start_date_dayofweek
    FROM ranked
    WHERE end_date >= DATE '2023-06-20'
)
SELECT
    DATE_TRUNC('YEAR', end_date) AS year_start,
    COUNT_IF(status = 'ACTIVE') AS active_events,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    ROUND(SUM(running_monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS running_monthly_fee_per_enterprise,
    COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count,
    ROUND(AVG(plan_count_start_date_dayofweek), 2) AS avg_plan_count_start_date_dayofweek
FROM valid
GROUP BY 1
