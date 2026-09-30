WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(sale_date, 82) sale_date_plus_82d
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-28'
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN revenue >= 3300 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
    product_id,
    PERCENTILE_APPROX(units, 0.9) AS units_p90,
    CORR(units, revenue) AS corr_units_revenue,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    BIT_AND(units) AS units_bit_and
FROM base
GROUP BY store_id, revenue_tier, product_id
ORDER BY revenue_tier DESC NULLS LAST
