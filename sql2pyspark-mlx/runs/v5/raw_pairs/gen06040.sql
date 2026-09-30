WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(unit_price, quantity) AS least_unit_price_quantity
    FROM order_items
    WHERE order_id IN (26, 29)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        quantity * unit_price AS line_total,
        quantity / NULLIF(least_unit_price_quantity, 0) AS quantity_per_least_unit_price_quantity
    FROM valid
    WHERE quantity > 3
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.quantity,
        src.least_unit_price_quantity,
        src.line_total,
        src.quantity_per_least_unit_price_quantity,
        d.price
    FROM filtered src
    INNER JOIN products d
        ON src.product_id = d.id
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        e.least_unit_price_quantity,
        e.quantity_per_least_unit_price_quantity,
        d.brand
    FROM with_dims e
    LEFT JOIN products d
        ON e.product_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(least_unit_price_quantity) OVER w_part AS least_unit_price_quantity_avg_5,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY least_unit_price_quantity ASC, id ASC) AS least_unit_price_quantity_bucket
    FROM joined
    WINDOW w_part AS (PARTITION BY brand ORDER BY quantity_per_least_unit_price_quantity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.least_unit_price_quantity,
        s.brand,
        b.product_name,
        b.launch_date
    FROM metrics s
    INNER JOIN products b
        ON s.product_id <=> b.id
    WHERE ARRAY_CONTAINS(b.tags, 'premium')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    CASE WHEN quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count,
    AVG(least_unit_price_quantity) FILTER (WHERE brand = 'Acme') AS acme_avg_least_unit_price_quantity
FROM with_dims_order_items
GROUP BY activity_month, quantity_tier, year_start
ORDER BY acme_avg_least_unit_price_quantity DESC NULLS LAST;
