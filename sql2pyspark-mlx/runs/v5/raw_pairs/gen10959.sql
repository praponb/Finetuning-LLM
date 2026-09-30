WITH with_windows AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        LAST_VALUE(revenue, TRUE) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        LEAD(units, 1) OVER w_grp AS next_units,
        SUM(units) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        AVG(units) OVER w_recent AS units_avg_5
    FROM store_sales
    WINDOW w_grp AS (ORDER BY revenue, id), w_recent AS (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN units >= 30 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
    store_id,
    ROUND(AVG(units_avg_5), 2) AS avg_units_avg_5,
    BIT_OR(units) AS units_bit_or,
    APPROX_PERCENTILE(units_avg_5, 0.95) AS units_avg_5_p95,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN units >= 30 THEN 'LARGE' ELSE 'SMALL' END ORDER BY APPROX_PERCENTILE(units_avg_5, 0.95) DESC) AS units_avg_5_p95_percent_rank
FROM with_windows
GROUP BY CASE WHEN units >= 30 THEN 'LARGE' ELSE 'SMALL' END, store_id;
