WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.order_id IN (10, 19)
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.city,
        s.country
    FROM kept a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        city,
        country,
        CAST(cost AS INT) AS cost_int,
        CASE WHEN stock_qty >= 428 THEN 'High' WHEN stock_qty >= 54 THEN 'Medium' ELSE 'Low' END AS stock_qty_tier,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM enriched
    WHERE launch_date > DATE '2023-09-03'
      AND supplier_id IS NOT NULL
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        city,
        country,
        cost_int,
        stock_qty_tier,
        country_filled,
        AVG(price) OVER (PARTITION BY city, category ORDER BY cost_int, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_avg_6,
        FIRST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        STDDEV_SAMP(cost_int) OVER w_hist AS country_stddev_samp_cost_int
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY country, city)
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT s.*, d.city AS supplier_city
    FROM enriched_products s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
),
stats AS (
    -- Grouped metrics
    SELECT
        country,
        city,
        COUNT(*) FILTER (WHERE brand = 'Stark') AS stark_rows,
        SIZE(COLLECT_SET(country)) AS num_countries,
        PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        ROUND(SUM(cost_int) / NULLIF(COUNT(CASE WHEN supplier_city = 'London' THEN 1 END), 0), 2) AS cost_int_per_london,
        APPROX_PERCENTILE(cost_int, ARRAY(0.25, 0.5, 0.75)) AS cost_int_quartiles
    FROM enriched_products_2
    GROUP BY ROLLUP (country, city)
    HAVING ROUND(SUM(cost_int) / NULLIF(COUNT(CASE WHEN supplier_city = 'London' THEN 1 END), 0), 2) > 0
)
SELECT
    country,
    stark_rows,
    num_countries,
    cost_quartiles,
    country_list,
    cost_int_per_london,
    cost_int_quartiles,
    CASE WHEN country RLIKE '^(UK|US)$' THEN 'EXTENDED' WHEN country RLIKE '^(TH|SG)$' THEN 'CORE' ELSE 'PRIORITY' END AS country_zone
FROM stats
ORDER BY country_zone DESC;
