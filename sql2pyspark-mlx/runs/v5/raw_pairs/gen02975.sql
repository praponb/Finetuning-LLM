WITH filtered AS (
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM subscriptions
    WHERE end_date >= DATE '2023-08-19'
),
enriched AS (
    SELECT e.*, d.city
    FROM filtered e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched_subscriptions AS (
    SELECT
        id,
        start_date,
        monthly_fee,
        city,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee,
        LEAD(monthly_fee, 2) OVER (PARTITION BY plan ORDER BY start_date, id) AS next_monthly_fee,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY city ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM enriched
    WHERE monthly_fee >= 210
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_monthly_fee, id) AS row_seq,
        LEAD(monthly_fee, 1) OVER w_grp AS next_monthly_fee_2,
        ROW_NUMBER() OVER w_grp AS event_seq,
        AVG(monthly_fee) OVER (PARTITION BY city ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3,
        LEAD(monthly_fee, 2) OVER (ORDER BY cumulative_monthly_fee, id) AS next_monthly_fee_3
    FROM enriched_subscriptions
    WINDOW w_grp AS (PARTITION BY city ORDER BY monthly_fee_bucket, id)
)
SELECT
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(*) AS num_rows,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    COUNT_IF(city = 'Singapore') AS singapore_events,
    COUNT(*) AS cnt
FROM metrics
GROUP BY DATE_TRUNC('QUARTER', start_date)
HAVING SIZE(COLLECT_SET(city)) > 1;
