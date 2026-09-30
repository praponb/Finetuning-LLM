WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label,
        ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2024-03-26'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY units ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_avg_6,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue ASC, id ASC) AS row_seq,
        SUM(revenue_per_unit) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_per_unit
    FROM filtered
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.sqft,
        b.store_name,
        b.opened_date
    FROM metrics src
    INNER JOIN stores b
        ON src.store_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN revenue_avg_6 >= 3179 THEN 'HIGH' ELSE 'LOW' END AS revenue_avg_6_tier,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    store_id,
    BIT_AND(units) AS units_bit_and,
    MAX(row_seq) AS max_row_seq,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50,
    BIT_AND(units) AS units_bit_and_2,
    BIT_AND(units) AS units_bit_and_3,
    MAX(revenue_per_unit) AS max_revenue_per_unit,
    GROUPING_ID() AS gid
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN revenue_avg_6 >= 3179 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', sale_date), store_id), (CASE WHEN revenue_avg_6 >= 3179 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', sale_date)), ())
ORDER BY sqft_p50 DESC
