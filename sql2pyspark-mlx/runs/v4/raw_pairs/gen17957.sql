WITH joined AS (
    -- Attach stores attributes
    SELECT e.*, s.opened_date
    FROM store_sales e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, p.product_name
    FROM joined a
    LEFT JOIN products p
        ON a.product_id = p.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(units * 100, 2) units_scaled,
        CEIL(units) units_whole
    FROM latest
    WHERE sale_date > DATE '2024-04-23'
)
-- Grouped metrics
SELECT
    CASE WHEN units_whole >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_whole_tier,
    SUM(revenue) AS total_revenue,
    ROUND(AVG(units_scaled), 2) AS avg_units_scaled,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    ROUND(AVG(units), 2) AS avg_units,
    ROUND(AVG(units_whole), 2) AS avg_units_whole
FROM cleaned
GROUP BY units_whole_tier
ORDER BY total_revenue
