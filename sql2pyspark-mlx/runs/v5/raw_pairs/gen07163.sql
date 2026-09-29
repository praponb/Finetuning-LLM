WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        revenue - units AS revenue_per_unit,
        CAST(units AS DOUBLE) AS units_num
    FROM store_sales
    WHERE sale_date >= DATE '2023-05-09'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        revenue_per_unit,
        units_num,
        LAG(revenue_per_unit, 2) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_revenue_per_unit,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS FIRST) AS units_dense_rank,
        LAST_VALUE(units_num, TRUE) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units_num,
        LAST_VALUE(units_num) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units_num_2,
        LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY revenue_per_unit, id) AS prev_revenue
    FROM valid
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(units * 0.9, 2) AS units_scaled
    FROM scored
    WHERE prev_revenue_per_unit > 36
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        units,
        revenue_per_unit,
        units_num,
        prev_revenue_per_unit,
        units_dense_rank,
        last_known_units_num,
        last_known_units_num_2,
        prev_revenue,
        units_scaled,
        CAST(last_known_units_num AS INT) AS last_known_units_num_int
    FROM base
    WHERE revenue_per_unit IS NULL
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_units_num_int >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_units_num_int_tier,
    CASE WHEN revenue_per_unit >= 74 THEN 'LARGE' ELSE 'SMALL' END AS revenue_per_unit_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids_4
FROM cleaned
GROUP BY CASE WHEN last_known_units_num_int >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN revenue_per_unit >= 74 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_ids_2 DESC
