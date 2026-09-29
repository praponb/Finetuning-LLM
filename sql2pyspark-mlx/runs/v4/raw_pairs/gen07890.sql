WITH base AS (
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE store_id IN (1, 2, 3, 6)
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY quantity DESC) AS quantity_dense_rank,
        LAG(quantity, 2) OVER (PARTITION BY store_id ORDER BY quantity, id) AS prev_quantity,
        FIRST_VALUE(quantity, TRUE) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM base
),
metrics_inventory AS (
    SELECT
        *,
        SUM(prev_quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_quantity,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY prev_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM metrics
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY last_restock_month_end DESC, id)
),
enriched AS (
    SELECT
        a.*,
        p.city,
        p.store_name
    FROM metrics_inventory a
    INNER JOIN stores p
        ON a.store_id = p.id
),
with_dims AS (
    SELECT
        a.*,
        s.region,
        s.city AS store_city
    FROM enriched a
    LEFT JOIN stores s
        ON a.store_id = s.id
)
SELECT
    CASE WHEN prev_quantity >= 248 THEN 'HIGH' ELSE 'LOW' END AS prev_quantity_tier,
    MIN(seq_no) AS min_seq_no,
    SUM(last_known_quantity) FILTER (WHERE region = 'East') AS east_total_last_known_quantity,
    COUNT_IF(region = 'West') AS west_events,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    SIZE(COLLECT_SET(region)) AS num_regions,
    COUNT(CASE WHEN store_city = 'Bangkok' THEN 1 END) AS bangkok_count_2
FROM with_dims
GROUP BY CASE WHEN prev_quantity >= 248 THEN 'HIGH' ELSE 'LOW' END
