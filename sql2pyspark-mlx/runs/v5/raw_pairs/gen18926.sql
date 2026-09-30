WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        cost,
        stock_qty,
        tags,
        COUNT(price) OVER w_part AS running_count_price,
        MAX(price) OVER w_part AS running_max_price,
        MIN(price) OVER w_part AS running_min_price,
        LEAD(price, 1) OVER w_grp AS next_price
    FROM products
    WINDOW w_part AS (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY supplier_id ORDER BY price, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (4, 5)
        )
),
with_windows_products AS (
    -- Running and moving aggregates
    SELECT
        id,
        brand,
        cost,
        next_price,
        AVG(next_price) OVER (PARTITION BY brand ORDER BY next_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_price,
        SUM(next_price) OVER (PARTITION BY brand ORDER BY next_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_price,
        MAX(next_price) OVER (PARTITION BY brand ORDER BY next_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_next_price
    FROM not_flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        brand,
        next_price,
        running_sum_next_price,
        running_max_next_price
    FROM with_windows_products
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY running_sum_next_price, id) = 1
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.revenue,
        latest.store_sale_units
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT brand, id
    FROM with_latest
    EXCEPT ALL
    SELECT brand, id
    FROM products
    WHERE product_name IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN kept_keys k ON s.brand = k.brand AND s.id = k.id
)
-- Grouped metrics
SELECT
    brand,
    CASE WHEN running_max_next_price >= 1020 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_next_price_tier,
    SUM(CASE WHEN brand = 'Globex' THEN running_sum_next_price ELSE 0 END) AS globex_running_sum_next_price,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    ROUND(SUM(running_sum_next_price) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS running_sum_next_price_per_stark,
    COUNT(*) AS row_count,
    AVG(store_sale_units) AS avg_store_sale_units,
    GROUPING(brand) AS grouping_level
FROM reconciled
GROUP BY CUBE (brand, CASE WHEN running_max_next_price >= 1020 THEN 'PREMIUM' ELSE 'STANDARD' END);
