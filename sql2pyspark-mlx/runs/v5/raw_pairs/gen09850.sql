WITH base AS (
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        TRIM(plan) AS clean_plan
    FROM subscriptions
    WHERE start_date >= TO_DATE('2023-06-08')
),
kept_keys AS (
    SELECT customer_id, plan
    FROM base
    EXCEPT ALL
    SELECT customer_id, plan
    FROM subscriptions
    WHERE monthly_fee BETWEEN 210 AND 220
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.plan = k.plan
),
filtered AS (
    SELECT *, LENGTH(plan) AS plan_length
    FROM verified
    WHERE start_date >= DATE '2023-09-23'
      AND customer_id IS NOT NULL
),
joined AS (
    SELECT
        a.*,
        p.signup_date,
        p.name,
        p.segment
    FROM filtered a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
cleaned AS (
    SELECT
        *,
        FLOOR(monthly_fee) AS monthly_fee_whole,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM joined
    WHERE end_date >= DATE '2023-09-18'
      AND plan_length BETWEEN 36 AND 38
)
SELECT
    CASE WHEN plan_length >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_length_tier,
    CASE WHEN monthly_fee >= 183 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    PERCENTILE_APPROX(plan_length, ARRAY(0.25, 0.5, 0.75)) AS plan_length_quartiles,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    APPROX_PERCENTILE(plan_length, 0.95) AS plan_length_p95,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN plan_length >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY ROUND(AVG(monthly_fee), 2) DESC) AS avg_monthly_fee_percent_rank
FROM cleaned
GROUP BY CASE WHEN plan_length >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN monthly_fee >= 183 THEN 'HIGH' ELSE 'LOW' END
