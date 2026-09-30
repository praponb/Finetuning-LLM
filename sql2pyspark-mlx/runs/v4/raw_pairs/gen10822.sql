WITH scored AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        FIRST_VALUE(revenue, TRUE) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue,
        CORR(revenue, units) OVER (PARTITION BY store_id, product_id) AS corr_revenue_units
    FROM store_sales
)
SELECT
    product_id,
    CASE WHEN corr_revenue_units >= 0.46 THEN 'HIGH' ELSE 'LOW' END AS corr_revenue_units_tier,
    BIT_OR(units) AS units_bit_or,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(first_known_revenue) AS total_first_known_revenue,
    CORR(revenue, units) AS corr_revenue_units
FROM scored
GROUP BY product_id, CASE WHEN corr_revenue_units >= 0.46 THEN 'HIGH' ELSE 'LOW' END
ORDER BY corr_revenue_units_tier DESC NULLS FIRST;
