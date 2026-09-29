WITH valid AS (
    SELECT
        *,
        LAST_DAY(launch_date) AS launch_date_month_end,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
      AND supplier_id IN (24, 39)
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.quantity IS NOT NULL
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.quantity IS NULL
        )
),
deduped AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        resolved_brand
    FROM (
        SELECT *, RANK() OVER (PARTITION BY supplier_id, category ORDER BY cost DESC) AS row_num
        FROM eligible
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    SELECT
        id,
        product_name,
        launch_date,
        AVG(cost) OVER w_recent AS running_avg_cost,
        MIN(cost) OVER w_recent AS running_min_cost,
        COUNT(cost) OVER w_recent AS running_count_cost,
        SUM(cost) OVER w_recent AS running_sum_cost,
        LEAD(cost, 2) OVER (PARTITION BY category ORDER BY cost, id) AS next_cost
    FROM deduped
    WHERE LENGTH(product_name) > 12
    WINDOW w_recent AS (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT id) AS distinct_ids_3,
    GROUPING(DATE_TRUNC('QUARTER', launch_date)) AS grouping_level
FROM enriched
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', launch_date), DATE_TRUNC('MONTH', launch_date)), (DATE_TRUNC('QUARTER', launch_date)), ());
