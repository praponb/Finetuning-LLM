WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(status) AS status_upper
    FROM subscriptions
    WHERE start_date > DATE '2023-09-04'
      AND status = 'PAUSED'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper_2,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM cleaned
    WHERE end_date > TO_DATE('2023-11-24')
      AND customer_id IS NOT NULL
),
base_subscriptions AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM base
    WHERE end_date >= DATE '2024-02-03'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS prev_monthly_fee,
        SUM(days_since_start_date) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_start_date,
        RANK() OVER (ORDER BY days_since_start_date DESC) AS days_since_start_date_rank,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id, status ORDER BY days_since_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        FIRST_VALUE(days_since_start_date) IGNORE NULLS OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_start_date
    FROM base_subscriptions
)
-- Multi-level aggregation
SELECT
    CASE WHEN days_since_start_date >= 86 THEN 'LARGE' ELSE 'SMALL' END AS days_since_start_date_tier,
    customer_id,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(first_known_days_since_start_date, 0.95) AS first_known_days_since_start_date_p95,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM scored
GROUP BY 1, 2
HAVING distinct_ids < 1
