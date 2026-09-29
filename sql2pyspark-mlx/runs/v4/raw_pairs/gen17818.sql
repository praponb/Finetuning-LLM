WITH valid AS (
    SELECT *, CASE WHEN plan IN ('enterprise', 'basic') THEN 'Group A' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        p.credit_limit,
        p.segment,
        p.city
    FROM valid e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
joined AS (
    SELECT e.*, c.age
    FROM enriched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        plan_group,
        credit_limit,
        segment,
        city,
        age,
        MAX(monthly_fee) OVER (PARTITION BY city) AS city_max_monthly_fee,
        ROUND(age / SUM(age) OVER (PARTITION BY status, customer_id), 4) AS age_share,
        DENSE_RANK() OVER (PARTITION BY customer_id, segment ORDER BY age) AS age_dense_rank,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        NTILE(3) OVER (PARTITION BY segment, city ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM joined
),
with_windows AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        plan_group,
        credit_limit,
        segment,
        city,
        age,
        city_max_monthly_fee,
        age_share,
        age_dense_rank,
        monthly_fee_bucket,
        credit_limit_bucket,
        AVG(credit_limit_bucket) OVER (PARTITION BY status, plan ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS credit_limit_bucket_avg_3,
        LAG(credit_limit, 1) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS prev_credit_limit,
        SUM(city_max_monthly_fee) OVER (PARTITION BY segment ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_city_max_monthly_fee
    FROM metrics
)
SELECT *
FROM (
    SELECT
        plan,
        segment,
        age_share
    FROM with_windows
) src
PIVOT (
    MAX(age_share) AS total,
    COUNT(1) AS item_count
    FOR segment IN ('Consumer', 'SMB')
);
