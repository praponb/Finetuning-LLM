WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(region) IGNORE NULLS OVER w_running first_known_region,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) sqft_avg_6,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) sqft_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) row_seq
    FROM stores
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM metrics
    WHERE opened_date >= DATE '2023-09-15'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 2, 4, 6)
        )
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN days_since_opened_date >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_opened_date_tier,
        PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
        STDDEV_SAMP(sqft) AS sqft_stddev,
        AVG(days_since_opened_date) AS avg_days_since_opened_date
    FROM kept
    GROUP BY days_since_opened_date_tier
    HAVING AVG(days_since_opened_date) < 0
)
SELECT
    days_since_opened_date_tier,
    sqft_quartiles,
    sqft_stddev,
    avg_days_since_opened_date
FROM daily
ORDER BY avg_days_since_opened_date DESC NULLS LAST
