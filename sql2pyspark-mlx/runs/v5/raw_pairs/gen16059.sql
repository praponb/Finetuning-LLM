WITH metrics AS (
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share
    FROM store_sales
    WHERE revenue IS NOT NULL
),
metrics_store_sales AS (
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        store_id_avg_revenue,
        revenue_share,
        FIRST_VALUE(revenue) IGNORE NULLS OVER w_running AS first_known_revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id, product_id), 4) AS units_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share_2,
        RANK() OVER w_grp AS store_id_avg_revenue_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY store_id_avg_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY store_id ORDER BY store_id_avg_revenue DESC)
),
metrics_store_sales_2 AS (
    SELECT
        id,
        store_id,
        revenue,
        store_id_avg_revenue,
        units_share,
        revenue_share_2,
        AVG(revenue) OVER (ORDER BY revenue_share_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS revenue_avg_3,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY store_id_avg_revenue_rank, id) AS store_id_avg_revenue_rank_bucket
    FROM metrics_store_sales
),
matched_keys AS (
    SELECT store_id
    FROM metrics_store_sales_2
    EXCEPT
    SELECT store_id
    FROM store_sales
    WHERE units >= 23
),
reconciled AS (
    SELECT s.*
    FROM metrics_store_sales_2 s
    INNER JOIN matched_keys k ON s.store_id = k.store_id
),
kept_keys AS (
    SELECT store_id
    FROM reconciled
    EXCEPT ALL
    SELECT store_id
    FROM store_sales
    WHERE product_id IN (1, 2, 8, 9)
),
reconciled_store_sales AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
),
joined AS (
    SELECT
        e.id,
        e.store_id,
        e.revenue,
        e.store_id_avg_revenue,
        e.revenue_share_2,
        e.revenue_avg_3,
        p.city
    FROM reconciled_store_sales e
    LEFT JOIN stores p
        ON e.store_id = p.id
)
SELECT
    store_id,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    AVG(revenue_share_2) AS avg_revenue_share_2,
    SUM(revenue_avg_3) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_total_revenue_avg_3,
    COUNT_IF(city = 'New York') AS new_york_events
FROM joined
GROUP BY 1
