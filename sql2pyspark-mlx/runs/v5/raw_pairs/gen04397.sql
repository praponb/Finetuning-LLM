WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        CAST(sqft AS STRING) AS sqft_str
    FROM stores
    WHERE opened_date >= DATE '2023-08-08'
      AND region IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.units
    FROM deduped s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS seq_no,
        ROW_NUMBER() OVER (ORDER BY units, id) AS seq_no_2,
        FIRST_VALUE(region) IGNORE NULLS OVER w_recent_2 AS first_known_region
    FROM latest_match
    WINDOW w_recent AS (PARTITION BY city ORDER BY units, id), w_recent_2 AS (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    region,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    APPROX_PERCENTILE(units, 0.5) AS units_p50,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS seq_no_per_tokyo,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS seq_no_per_south
FROM with_windows
GROUP BY region, DATE_TRUNC('YEAR', opened_date)
HAVING APPROX_PERCENTILE(units, 0.5) >= 0
ORDER BY units_p50
