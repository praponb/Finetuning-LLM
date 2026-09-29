WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        revenue,
        DAY(sale_date) sale_date_day
    FROM store_sales
    WHERE product_id IN (4, 10)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY sale_date_day ASC NULLS FIRST) sale_date_day_rank,
        RANK() OVER (ORDER BY sale_date_day ASC NULLS LAST) sale_date_day_rank_2,
        SUM(revenue) OVER (ORDER BY sale_date_day ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        ROW_NUMBER() OVER (ORDER BY sale_date_day ASC, id ASC) seq_no
    FROM cleaned
    WHERE revenue BETWEEN 2900 AND 3000
),
cleaned_store_sales AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(sale_date_day AS STRING) sale_date_day_str
    FROM enriched
    WHERE sale_date_day_rank <= 5
),
cleaned_store_sales_2 AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cumulative_revenue AS STRING) cumulative_revenue_str
    FROM cleaned_store_sales
    WHERE sale_date_day_rank_2 < 7
),
cleaned_store_sales_3 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        revenue,
        sale_date_day,
        sale_date_day_rank,
        sale_date_day_rank_2,
        cumulative_revenue,
        seq_no,
        sale_date_day_str,
        FLOOR(cumulative_revenue) cumulative_revenue_whole
    FROM cleaned_store_sales_2
    WHERE seq_no != 4
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_revenue >= 19608 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_revenue_tier,
    ROUND(AVG(cumulative_revenue), 2) AS avg_cumulative_revenue,
    APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(*) AS cnt
FROM cleaned_store_sales_3
GROUP BY CASE WHEN cumulative_revenue >= 19608 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cumulative_revenue_tier ASC NULLS LAST
