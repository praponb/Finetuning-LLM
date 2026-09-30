WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(city, 'unassigned') AS city_filled,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.city_clean,
        top1.launch_date,
        top1.stock_qty
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date < s.launch_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(stock_qty) OVER (PARTITION BY city ORDER BY stock_qty, id) AS prev_stock_qty,
        LEAD(stock_qty) OVER (PARTITION BY city ORDER BY stock_qty, id) AS next_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY stock_qty, id) AS event_seq
    FROM not_flagged
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty <= 243
        )
),
kept_suppliers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date < s.launch_date
        )
),
not_flagged_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category IN ('Home', 'Sports')
        )
)
SELECT
    id,
    supplier_name,
    city_clean,
    launch_date,
    stock_qty,
    prev_stock_qty,
    event_seq,
    CASE WHEN supplier_name RLIKE '^(Tokyo Traders|Global Supply)$' THEN 'GROUP_A' WHEN supplier_name RLIKE '^(Siam Parts Co|EuroGoods GmbH)$' THEN 'EXTENDED' ELSE 'CORE' END AS supplier_name_zone
FROM not_flagged_suppliers
ORDER BY id NULLS FIRST, event_seq
