WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(revenue, 0) AS revenue_filled,
        COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-16'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(units) OVER (PARTITION BY product_id) AS product_id_max_units,
        FIRST_VALUE(revenue_filled, TRUE) OVER (PARTITION BY store_id ORDER BY revenue_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue_filled,
        AVG(revenue) OVER (ORDER BY units_filled, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY units_filled, id) AS units_filled_bucket
    FROM cleaned
),
cleaned_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_filled,
        units_filled,
        product_id_max_units,
        first_known_revenue_filled,
        units_filled_bucket,
        ROUND(units * 0.9, 2) AS units_scaled,
        units_filled - units AS units_filled_minus_units
    FROM scored
    WHERE revenue_filled < 32
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        sale_date,
        units,
        units_filled,
        product_id_max_units,
        units_filled_bucket,
        units_filled_minus_units,
        units - first_known_revenue_filled AS units_minus_first_known_revenue_filled,
        ROUND(units * 0.85, 2) AS units_scaled_2
    FROM cleaned_store_sales
    WHERE sale_date > TO_DATE('2023-10-12')
      AND units BETWEEN 16 AND 34
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS latest_rank
        FROM base
    ) t
    WHERE latest_rank = 1
),
grouped AS (
    -- Grouped metrics
    SELECT
        product_id,
        CASE WHEN units_minus_first_known_revenue_filled >= 84 THEN 'HIGH' ELSE 'LOW' END AS units_minus_first_known_revenue_filled_tier,
        PERCENTILE_APPROX(units_scaled_2, 0.5) AS units_scaled_2_p50,
        AVG(units_filled_bucket) AS avg_units_filled_bucket,
        APPROX_PERCENTILE(units_filled_minus_units, 0.95) AS units_filled_minus_units_p95,
        COUNT(*) AS cnt,
        COUNT(*) AS row_count
    FROM latest
    GROUP BY CUBE (product_id, CASE WHEN units_minus_first_known_revenue_filled >= 84 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    s.product_id,
    s.units_minus_first_known_revenue_filled_tier,
    s.units_scaled_2_p50,
    s.avg_units_filled_bucket,
    s.units_filled_minus_units_p95,
    s.cnt,
    s.row_count,
    (SELECT MIN(s2.revenue) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS min_store_sale_revenue
FROM grouped s
