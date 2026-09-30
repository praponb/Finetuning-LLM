WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label,
        CAST(started_at AS DATE) AS started_at_day
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-08-30 09:00:00'
      AND browser IS NOT NULL
      AND country IN ('US', 'UK')
),
base_sessions AS (
    SELECT
        *,
        LAST_DAY(started_at_day) AS started_at_day_month_end,
        CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg,
        COALESCE(browser, 'UNKNOWN') AS browser_filled
    FROM base
    WHERE country = 'TH'
),
base_sessions_2 AS (
    SELECT
        id,
        device,
        started_at,
        started_at_label,
        is_sg,
        browser_filled,
        TRIM(device) AS clean_device,
        INITCAP(TRIM(device)) AS device_title,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM base_sessions
    WHERE started_at > DATE '2023-12-14'
      AND YEAR(started_at_day_month_end) = 2025
)
SELECT
    CASE WHEN is_sg >= 65 THEN 'LARGE' ELSE 'SMALL' END AS is_sg_tier,
    device,
    MAX(is_sg) AS max_is_sg,
    APPROX_PERCENTILE(is_sg, ARRAY(0.25, 0.5, 0.75)) AS is_sg_quartiles,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    MIN(is_sg) AS min_is_sg,
    APPROX_PERCENTILE(is_sg, 0.5) AS is_sg_p50,
    ROUND(SUM(is_sg) / NULLIF(COUNT(CASE WHEN device = 'mobile' THEN 1 END), 0), 2) AS is_sg_per_mobile,
    RANK() OVER (PARTITION BY CASE WHEN is_sg >= 65 THEN 'LARGE' ELSE 'SMALL' END ORDER BY APPROX_PERCENTILE(is_sg, 0.5) DESC) AS is_sg_p50_rank
FROM base_sessions_2
GROUP BY CASE WHEN is_sg >= 65 THEN 'LARGE' ELSE 'SMALL' END, device
HAVING ROUND(SUM(is_sg) / NULLIF(COUNT(CASE WHEN device = 'mobile' THEN 1 END), 0), 2) < 0
ORDER BY min_is_sg DESC NULLS LAST;
