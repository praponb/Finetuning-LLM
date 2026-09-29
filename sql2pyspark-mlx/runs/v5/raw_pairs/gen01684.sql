WITH valid AS (
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
        LAST_DAY(end_date) AS end_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND start_date <= '2025-10-01'
),
remaining_keys AS (
    SELECT status, plan
    FROM valid
    EXCEPT ALL
    SELECT status, plan
    FROM subscriptions
    WHERE customer_id IN (10, 18, 20, 36)
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.plan = k.plan
),
filtered AS (
    SELECT
        id,
        customer_id,
        monthly_fee,
        status,
        start_date_label,
        TRIM(status) AS clean_status,
        DATE_FORMAT(end_date_month_end, 'MMM yyyy') AS end_date_month_end_label
    FROM kept_rows
    WHERE customer_id IS NOT NULL
),
with_dims AS (
    SELECT
        e.id,
        e.customer_id,
        e.end_date_month_end_label,
        p.email,
        p.credit_limit
    FROM filtered e
    LEFT JOIN customers p
        ON e.customer_id = p.id
)
SELECT
    p.country,
    p.segment,
    COUNT(*) AS rows_total,
    ROUND(AVG(src.credit_limit), 2) AS avg_credit_limit
FROM with_dims src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, p.segment), (p.country), ())
ORDER BY rows_total NULLS FIRST
