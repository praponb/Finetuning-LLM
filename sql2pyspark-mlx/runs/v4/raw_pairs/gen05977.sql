WITH base AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        CASE WHEN units >= 41 THEN 'High' WHEN units >= 12 THEN 'Medium' ELSE 'Low' END AS units_level
    FROM store_sales
    WHERE sale_date >= DATE '2024-04-16'
),
joined AS (
    SELECT
        src.*,
        d.city,
        d.opened_date
    FROM base src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
with_dims AS (
    SELECT e.*, d.city AS store_city
    FROM joined e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
valid AS (
    SELECT
        id,
        units,
        revenue,
        units_level,
        opened_date,
        store_city,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM with_dims
    WHERE sale_date > DATE '2024-01-01'
      AND store_city IS NOT NULL
),
daily AS (
    SELECT
        CASE WHEN revenue >= 3300 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
        STDDEV_SAMP(units) AS units_stddev,
        PERCENTILE_APPROX(revenue, 0.9) AS revenue_p90,
        COUNT(*) FILTER (WHERE store_city = 'Chiang Mai') AS chiang_mai_rows,
        AVG(revenue) FILTER (WHERE store_city = 'Tokyo') AS tokyo_avg_revenue,
        SORT_ARRAY(COLLECT_SET(store_city)) AS store_city_list,
        SORT_ARRAY(COLLECT_SET(store_city)) AS store_city_list_2
    FROM valid
    GROUP BY revenue_tier
)
SELECT
    revenue_tier,
    units_stddev,
    revenue_p90,
    tokyo_avg_revenue,
    store_city_list,
    store_city_list_2
FROM daily
ORDER BY revenue_tier DESC;
