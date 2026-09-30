WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.name,
        b.country
    FROM valid s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        days_start_date_to_end_date,
        name,
        country,
        DENSE_RANK() OVER (ORDER BY days_start_date_to_end_date DESC) AS days_start_date_to_end_date_dense_rank,
        NTILE(5) OVER (PARTITION BY country ORDER BY days_start_date_to_end_date, id) AS days_start_date_to_end_date_bucket
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN days_start_date_to_end_date_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS days_start_date_to_end_date_bucket_tier,
    customer_id,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS monthly_fee_per_cancelled,
    ROUND(AVG(days_start_date_to_end_date), 2) AS avg_days_start_date_to_end_date,
    SUM(CASE WHEN country = 'DE' THEN days_start_date_to_end_date ELSE 0 END) AS de_days_start_date_to_end_date,
    APPROX_PERCENTILE(days_start_date_to_end_date_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS days_start_date_to_end_date_dense_rank_quartiles,
    COUNT(DISTINCT plan) AS distinct_plans
FROM with_windows
GROUP BY 1, 2
ORDER BY de_days_start_date_to_end_date DESC
