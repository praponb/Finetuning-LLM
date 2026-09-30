WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(units, 1) AS units_rounded
    FROM store_sales
    WHERE sale_date > DATE '2024-01-14'
      AND sale_date BETWEEN '2023-12-01' AND '2025-09-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(units) AS units_whole
    FROM filtered
    WHERE sale_date >= DATE '2024-04-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units_rounded,
        units_whole,
        FIRST_VALUE(units_rounded) IGNORE NULLS OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units_rounded,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue, id) AS seq_no
    FROM base
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    product_id,
    BIT_AND(units) AS units_bit_and,
    STDDEV_SAMP(units) AS units_stddev,
    PERCENTILE_APPROX(units_whole, ARRAY(0.25, 0.5, 0.75)) AS units_whole_quartiles
FROM enriched
GROUP BY year_start, product_id
ORDER BY year_start NULLS LAST
