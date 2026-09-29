WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(status, ', ', plan) AS status_plan
    FROM subscriptions
    WHERE start_date > DATE '2023-08-24'
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, p.age
    FROM valid s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM joined
    EXCEPT
    SELECT customer_id
    FROM subscriptions
    WHERE plan IN ('standard', 'enterprise', 'basic')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.customer_id = k.customer_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM verified
    WHERE end_date > DATE '2024-04-10'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY age DESC NULLS FIRST) AS age_rank,
        DENSE_RANK() OVER w_grp AS monthly_fee_dense_rank
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY monthly_fee NULLS LAST)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    DATE_TRUNC('WEEK', start_date) AS week_start,
    AVG(monthly_fee_dense_rank) AS avg_monthly_fee_dense_rank,
    ROUND(AVG(age), 2) AS avg_age,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(age_rank, ARRAY(0.25, 0.5, 0.75)) AS age_rank_quartiles,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee,
    ROUND(SUM(monthly_fee_dense_rank) / NULLIF(COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END), 0), 2) AS monthly_fee_dense_rank_per_expired,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY ROUND(AVG(age), 2) DESC) AS avg_age_dense_rank
FROM enriched
GROUP BY DATE_TRUNC('MONTH', end_date), DATE_TRUNC('WEEK', start_date)
ORDER BY monthly_fee_dense_rank_per_expired NULLS LAST;
