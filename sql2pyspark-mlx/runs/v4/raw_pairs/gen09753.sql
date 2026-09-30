WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(plan) AS plan_lower
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-06-29')
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM filtered
    EXCEPT ALL
    SELECT customer_id
    FROM subscriptions
    WHERE customer_id IN (1, 40)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.country,
        p.name
    FROM verified a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share,
        MAX(monthly_fee) OVER w_recent AS customer_id_max_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share_2,
        COUNT(monthly_fee) OVER (PARTITION BY status) AS status_count_monthly_fee
    FROM joined
    WINDOW w_recent AS (PARTITION BY customer_id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        plan_lower,
        country,
        name,
        monthly_fee_share,
        customer_id_max_monthly_fee,
        monthly_fee_share_2,
        status_count_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, plan), 4) AS monthly_fee_share_3,
        MIN(status_count_monthly_fee) OVER w_running AS country_min_status_count_monthly_fee
    FROM scored
    WINDOW w_running AS (PARTITION BY country, customer_id)
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    status,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT(*) AS event_count,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    ROUND(SUM(monthly_fee_share_3) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS monthly_fee_share_3_per_sg,
    MIN(monthly_fee_share_2) AS min_monthly_fee_share_2,
    AVG(monthly_fee_share_2) FILTER (WHERE country = 'US') AS us_avg_monthly_fee_share_2,
    RANK() OVER (PARTITION BY customer_id ORDER BY MIN(monthly_fee_share_2) DESC) AS min_monthly_fee_share_2_rank
FROM metrics
GROUP BY customer_id, status
HAVING STDDEV_SAMP(monthly_fee) > 0
ORDER BY monthly_fee_stddev
