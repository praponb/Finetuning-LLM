WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(starts_on, 'MMM yyyy') AS starts_on_label
    FROM promotions
    WHERE starts_on > DATE '2023-12-19'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(discount_pct, 1) AS discount_pct_rounded
    FROM valid
    WHERE ends_on BETWEEN '2023-09-01' AND '2025-02-02'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_label,
        DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label,
        DATE_FORMAT(starts_on, 'yyyy-MM-dd') AS starts_on_label_2
    FROM base
    WHERE YEAR(starts_on) >= 2025
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
        ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(discount_pct, 0.95) AS discount_pct_p95,
        PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles
    FROM cleaned
    GROUP BY DATE_TRUNC('MONTH', ends_on), DATE_TRUNC('QUARTER', ends_on)
)
SELECT
    activity_month,
    quarter_start,
    avg_discount_pct
FROM summary
ORDER BY quarter_start DESC NULLS FIRST
