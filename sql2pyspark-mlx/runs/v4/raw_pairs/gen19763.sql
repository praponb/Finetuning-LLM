WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(product_name)) AS product_name_normalized,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-05-11'
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        s.rating,
        s.supplier_name,
        s.city
    FROM filtered a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IS NOT NULL
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_stock_qty,
        ROW_NUMBER() OVER w_recent AS row_seq,
        LAG(price, 1) OVER (PARTITION BY category ORDER BY cost, id) AS prev_price
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY city ORDER BY launch_date, id)
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_last_restock,
        top1.quantity
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(price) OVER (PARTITION BY supplier_id) AS supplier_id_stddev_samp_price,
        NTILE(10) OVER w_running AS prev_price_bucket
    FROM with_latest
    WINDOW w_running AS (PARTITION BY category ORDER BY prev_price DESC, id)
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        d.supplier_name AS supplier_supplier_name,
        d.rating AS supplier_rating,
        d.city AS supplier_city
    FROM metrics s
    INNER JOIN suppliers d
        ON s.supplier_id = d.id
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags
FROM with_dims
ORDER BY category, price
