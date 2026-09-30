WITH base AS (
    SELECT
        *,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        ROUND(monthly_fee * 100, 2) AS monthly_fee_scaled
    FROM subscriptions
    WHERE start_date >= DATE '2023-07-13'
      AND customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY end_date, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST, id) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
daily AS (
    SELECT
        customer_id,
        DATE_TRUNC('MONTH', start_date) AS activity_month,
        MAX(monthly_fee) FILTER (WHERE plan = 'basic') AS basic_max_monthly_fee,
        SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        GROUPING(customer_id) AS grouping_id
    FROM deduped
    GROUP BY GROUPING SETS ((customer_id, DATE_TRUNC('MONTH', start_date)), (customer_id))
    HAVING enterprise_monthly_fee >= 0
)
SELECT
    basic_max_monthly_fee,
    enterprise_monthly_fee,
    status_list,
    grouping_id
FROM daily
