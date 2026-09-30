WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        LAST_DAY(launch_date) AS launch_date_month_end,
        CASE WHEN brand IN ('Initech', 'Umbrella') THEN 'Core' ELSE 'Other' END AS brand_group
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        launch_date,
        launch_date_month_end,
        brand_group,
        CORR(stock_qty, price) OVER w_ordered AS corr_stock_qty_price,
        FIRST_VALUE(stock_qty, TRUE) OVER w_part AS first_known_stock_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
        CORR(price, stock_qty) OVER w_ordered AS corr_price_stock_qty,
        LAG(price, 1) OVER w_recent AS prev_price
    FROM base
    WINDOW w_ordered AS (PARTITION BY brand), w_part AS (PARTITION BY category, brand ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY stock_qty, id)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.launch_date,
        s.brand_group,
        s.first_known_stock_qty,
        s.price_share,
        s.corr_price_stock_qty,
        top1.quantity
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.launch_date,
        s.first_known_stock_qty,
        s.corr_price_stock_qty,
        s.quantity,
        x.unit_price
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.quantity > 114
        )
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible_products AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(unit_price, 0.5) AS unit_price_p50,
        COUNT(*) AS cnt,
        MAX(unit_price) AS max_unit_price,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM eligible_products
    GROUP BY CUBE (CASE WHEN unit_price >= 380 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', launch_date))
)
SELECT
    week_start,
    num_rows,
    max_unit_price,
    distinct_ids_2
FROM stats
ORDER BY distinct_ids_2 DESC;
