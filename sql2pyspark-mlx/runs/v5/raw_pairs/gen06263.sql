SELECT
    CASE WHEN units >= 35 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
    BIT_OR(units) AS units_bit_or,
    CORR(units, revenue) AS corr_units_revenue,
    AVG(revenue) AS avg_revenue,
    AVG(units) AS avg_units,
    APPROX_PERCENTILE(units, 0.95) AS units_p95
FROM store_sales
GROUP BY 1
HAVING CORR(units, revenue) > 0;
