WITH enriched AS (
    SELECT
        *,
        LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        LEAD(units, 2) OVER w_recent AS next_units,
        LAG(units, 1) OVER w_hist AS prev_units
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY revenue ASC, id ASC), w_hist AS (PARTITION BY product_id ORDER BY sale_date ASC, id ASC)
)
SELECT
    store_id,
    CASE WHEN last_known_revenue >= 3300 THEN 'HIGH' ELSE 'LOW' END AS last_known_revenue_tier,
    ROUND(AVG(prev_units), 2) AS avg_prev_units,
    BIT_AND(units) AS units_bit_and,
    STDDEV_SAMP(units) AS units_stddev,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY ROUND(AVG(prev_units), 2) DESC) AS avg_prev_units_percent_rank
FROM enriched
GROUP BY store_id, CASE WHEN last_known_revenue >= 3300 THEN 'HIGH' ELSE 'LOW' END
HAVING units_stddev < 0
ORDER BY avg_prev_units ASC;
