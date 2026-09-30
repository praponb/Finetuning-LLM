WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(unit_price * 1.1, 2) AS adjusted_unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE unit_price >= 110
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM filtered
    EXCEPT
    SELECT product_id, id
    FROM order_items
    WHERE product_id IN (8, 17, 23, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM reconciled
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.order_id,
        e.product_id,
        e.unit_price,
        e.quantity_tier,
        o.status,
        o.amount,
        o.discount
    FROM valid e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
joined_order_items AS (
    -- Attach products attributes
    SELECT
        s.*,
        b.brand,
        b.product_name,
        b.stock_qty
    FROM joined s
    LEFT JOIN products b
        ON s.product_id <=> b.id
    WHERE b.category <> 'Grocery'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, product_id
    FROM joined_order_items
    EXCEPT ALL
    SELECT order_id, product_id
    FROM order_items
    WHERE unit_price > 210
),
reconciled_order_items AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined_order_items s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.product_id = k.product_id
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        order_id,
        unit_price,
        quantity_tier,
        status,
        brand,
        stock_qty,
        INITCAP(TRIM(product_name)) AS product_name_title,
        LOWER(brand) AS brand_lower,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM reconciled_order_items
    WHERE product_id IS NOT NULL
)
SELECT
    s.order_id,
    s.unit_price,
    s.quantity_tier,
    s.status,
    s.brand,
    s.product_name_title,
    s.brand_lower,
    s.status_filled,
    CASE WHEN brand RLIKE '^(Initech|Stark)$' THEN 'CORE' WHEN brand RLIKE '^(Acme|Globex)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS brand_zone,
    (SELECT MIN(t2.unit_price) FROM order_items t2 WHERE t2.order_id = s.order_id) AS min_order_item_unit_price
FROM filtered_order_items s
ORDER BY min_order_item_unit_price NULLS LAST;
