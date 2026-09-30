WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM store_sales
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY revenue NULLS LAST) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        AVG(units) OVER (ORDER BY units, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_avg_3,
        CORR(revenue, units) OVER (PARTITION BY product_id, store_id) AS corr_revenue_units,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share
    FROM latest
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY units DESC, id)
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        s.*,
        b.city,
        b.opened_date,
        b.sqft
    FROM enriched s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(sale_date, opened_date) AS days_opened_date_to_sale_date,
        ROUND(revenue, 0) AS revenue_rounded,
        COALESCE(city, 'N/A') AS city_filled
    FROM enriched_store_sales
    WHERE store_id IN (1, 4, 5)
),
enriched_store_sales_2 AS (
    -- Attach stores attributes
    SELECT e.*, d.city AS store_city
    FROM cleaned e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.product_name,
        p.launch_date
    FROM enriched_store_sales_2 s
    INNER JOIN products p
        ON s.product_id = p.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN price >= 1270 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
        city,
        APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
        COUNT_IF(city = 'New York') AS new_york_events,
        SORT_ARRAY(COLLECT_SET(store_city)) AS store_city_list
    FROM with_dims
    GROUP BY CASE WHEN price >= 1270 THEN 'PREMIUM' ELSE 'STANDARD' END, city
)
SELECT
    price_tier,
    city,
    price_quartiles,
    new_york_events,
    store_city_list,
    CASE WHEN city RLIKE '^(London|Berlin)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS city_zone
FROM grouped
ORDER BY city NULLS LAST, city_zone
