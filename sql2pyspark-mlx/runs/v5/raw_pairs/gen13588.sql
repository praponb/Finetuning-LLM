WITH filtered AS (
    SELECT *, ROUND(monthly_fee, 1) monthly_fee_rounded
    FROM subscriptions
    WHERE plan = 'enterprise'
),
matched_keys AS (
    SELECT plan, status
    FROM filtered
    EXCEPT
    SELECT plan, status
    FROM subscriptions
    WHERE start_date BETWEEN '2023-06-21' AND '2024-09-01'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.status = k.status
),
joined AS (
    SELECT a.*, d.name
    FROM reconciled a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
),
cleaned AS (
    SELECT
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_rounded,
        name,
        UPPER(status) status_upper
    FROM joined
    WHERE customer_id IS NOT NULL
)
SELECT
    DATE_TRUNC('MONTH', start_date) AS activity_month,
    COUNT(*) AS event_count,
    AVG(monthly_fee) AS avg_monthly_fee,
    COUNT(*) AS row_count,
    AVG(monthly_fee_rounded) FILTER (WHERE status = 'EXPIRED') AS expired_avg_monthly_fee_rounded,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', start_date)
HAVING row_count >= 1;
