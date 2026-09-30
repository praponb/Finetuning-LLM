WITH enriched AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        MIN(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_units,
        AVG(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_units,
        SUM(units) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_units
    FROM store_sales
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS FIRST) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
joined AS (
    SELECT
        e.*,
        p.sqft,
        p.city
    FROM deduped e
    INNER JOIN stores p
        ON e.store_id = p.id
),
cleaned AS (
    SELECT *, running_avg_units * running_sum_units AS running_avg_units_times_running_sum_units
    FROM joined
    WHERE running_min_units IS NOT NULL
)
SELECT
    CASE WHEN running_avg_units >= 42 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_units_tier,
    MIN(running_min_units) AS min_running_min_units,
    PERCENTILE_APPROX(running_avg_units_times_running_sum_units, ARRAY(0.25, 0.5, 0.75)) AS running_avg_units_times_running_sum_units_quartiles,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
    SUM(CASE WHEN city = 'Berlin' THEN units ELSE 0 END) AS berlin_units
FROM cleaned
GROUP BY 1
HAVING berlin_rows > 1;
