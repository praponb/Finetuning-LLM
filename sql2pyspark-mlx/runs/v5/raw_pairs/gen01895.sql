WITH base AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date >= DATE '2023-06-17'
),
matched AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM base e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL 7 DAYS
),
latest AS (
    SELECT *
    FROM matched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY start_date, id, subscription_id) = 1
),
joined AS (
    SELECT
        e.id,
        e.customer_id,
        e.start_date,
        e.end_date,
        e.status,
        e.subscription_id,
        e.subscription_plan,
        e.subscription_monthly_fee,
        p.email,
        p.credit_limit,
        p.name
    FROM latest e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
scored AS (
    SELECT
        id,
        customer_id,
        end_date,
        subscription_id,
        credit_limit,
        name,
        SUM(subscription_monthly_fee) OVER (ORDER BY credit_limit, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_subscription_monthly_fee,
        AVG(subscription_id) OVER (PARTITION BY customer_id ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS subscription_id_avg_5
    FROM joined
    WHERE subscription_monthly_fee BETWEEN 90 AND 210
)
SELECT
    subscription_id,
    MAX(credit_limit) AS max_credit_limit,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    COUNT(*) AS row_count,
    COUNT(*) AS num_rows
FROM scored
GROUP BY 1
HAVING MAX(credit_limit) > 0
ORDER BY subscription_id NULLS LAST;
