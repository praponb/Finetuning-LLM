WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date >= DATE '2023-09-25'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units,
        revenue,
        AVG(units) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY sale_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue,
        LEAD(revenue, 1) OVER (PARTITION BY store_id ORDER BY units, id) AS next_revenue,
        CORR(revenue, units) OVER (PARTITION BY store_id, product_id) AS corr_revenue_units
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, revenue - units AS revenue_per_unit
    FROM enriched
    WHERE revenue <= 3300
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        units,
        revenue,
        first_known_revenue,
        corr_revenue_units,
        revenue_per_unit,
        LEAD(units_avg_6) OVER w_recent AS next_units_avg_6,
        LAG(units_avg_6) OVER w_recent AS prev_units_avg_6,
        LEAD(first_known_revenue) OVER w_recent AS next_first_known_revenue,
        LAG(first_known_revenue) OVER w_recent AS prev_first_known_revenue,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM valid
    WINDOW w_recent AS (ORDER BY next_revenue, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_first_known_revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END AS prev_first_known_revenue_tier,
        PERCENTILE_APPROX(first_known_revenue, 0.9) AS first_known_revenue_p90,
        ROUND(AVG(next_units_avg_6), 2) AS avg_next_units_avg_6,
        BIT_AND(units) AS units_bit_and,
        GROUPING(CASE WHEN prev_first_known_revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN prev_first_known_revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    prev_first_known_revenue_tier,
    first_known_revenue_p90,
    avg_next_units_avg_6,
    units_bit_and
FROM totals;
