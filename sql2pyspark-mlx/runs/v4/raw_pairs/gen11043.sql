WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS LAST, id) AS rn
        FROM store_sales
    ) ranked
    WHERE rn = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY rn DESC, id) AS rn_2
    FROM deduped
    QUALIFY rn_2 = 1
),
enriched AS (
    SELECT
        a.*,
        d.opened_date,
        d.city,
        d.store_name
    FROM ranked a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
enriched_store_sales AS (
    SELECT
        src.*,
        b.stock_qty,
        b.product_name,
        b.brand
    FROM enriched src
    LEFT JOIN products b
        ON src.product_id = b.id
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        rn,
        rn_2,
        opened_date,
        city,
        store_name,
        stock_qty,
        product_name,
        brand,
        NTILE(10) OVER (PARTITION BY product_id, store_id ORDER BY stock_qty, id) AS stock_qty_bucket,
        ROW_NUMBER() OVER w_hist AS seq_no,
        SUM(revenue) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue
    FROM enriched_store_sales
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY stock_qty, id)
),
enriched_store_sales_2 AS (
    SELECT
        e.*,
        b.region,
        b.sqft,
        b.city AS store_city
    FROM metrics e
    INNER JOIN stores b
        ON e.store_id = b.id
),
summary AS (
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        COUNT_IF(region = 'West') AS west_events,
        AVG(running_revenue) FILTER (WHERE city = 'New York') AS new_york_avg_running_revenue,
        APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
        GROUPING_ID() AS gid
    FROM enriched_store_sales_2
    GROUP BY CUBE (DATE_TRUNC('WEEK', opened_date))
    HAVING COUNT_IF(region = 'West') < 1
)
SELECT week_start, units_quartiles
FROM summary
ORDER BY week_start;
