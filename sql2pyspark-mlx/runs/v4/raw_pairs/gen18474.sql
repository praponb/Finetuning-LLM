WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.unit_price > 280
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, brand
    FROM kept
    EXCEPT ALL
    SELECT id, brand
    FROM products
    WHERE supplier_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.id = k.id AND s.brand = k.brand
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p2.rating,
        p2.city,
        p2.supplier_name
    FROM reconciled s
    INNER JOIN suppliers p2
        ON s.supplier_id = p2.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        rating + price AS rating_plus_price,
        SUBSTRING(category, 1, 3) AS category_prefix
    FROM enriched
    WHERE launch_date > DATE '2023-11-14'
      AND SIZE(tags) >= 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY category, brand), 4) AS cost_share,
        ROUND(price / SUM(price) OVER (PARTITION BY brand, category), 4) AS price_share
    FROM base
),
kept_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.unit_price > 330
        )
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT src.*, b.country
    FROM kept_products src
    LEFT JOIN suppliers b
        ON src.supplier_id = b.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        COUNT_IF(category = 'Fashion') AS fashion_events,
        APPROX_PERCENTILE(rating_plus_price, 0.95) AS rating_plus_price_p95
    FROM enriched_products
    GROUP BY DATE_TRUNC('QUARTER', launch_date), DATE_TRUNC('MONTH', launch_date)
    HAVING COUNT_IF(category = 'Fashion') < 1
)
SELECT
    quarter_start,
    activity_month,
    fashion_events,
    rating_plus_price_p95
FROM totals;
