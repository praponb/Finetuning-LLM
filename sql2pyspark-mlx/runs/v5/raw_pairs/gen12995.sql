WITH valid AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_ADD(end_date, 19) AS end_date_plus_19d
    FROM subscriptions
    WHERE end_date >= DATE '2023-10-18'
      AND customer_id IS NOT NULL
),
matched_keys AS (
    SELECT id, status
    FROM valid
    EXCEPT DISTINCT
    SELECT id, status
    FROM subscriptions
    WHERE start_date BETWEEN '2023-10-01' AND '2025-10-01'
),
verified AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
base AS (
    SELECT
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        INITCAP(TRIM(status)) AS status_title,
        LAST_DAY(start_date) AS start_date_month_end
    FROM verified
    WHERE plan = 'premium'
),
cleaned AS (
    SELECT
        end_date,
        monthly_fee,
        status,
        status_title,
        start_date_month_end,
        LAST_DAY(start_date_month_end) AS start_date_month_end_month_end,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM base
    WHERE monthly_fee > 240
)
SELECT
    CASE WHEN monthly_fee >= 216 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    DATE_TRUNC('MONTH', start_date_month_end) AS activity_month,
    DATE_TRUNC('QUARTER', start_date_month_end) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles
FROM cleaned
GROUP BY CASE WHEN monthly_fee >= 216 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', start_date_month_end), DATE_TRUNC('QUARTER', start_date_month_end)
HAVING active_count < 1000
