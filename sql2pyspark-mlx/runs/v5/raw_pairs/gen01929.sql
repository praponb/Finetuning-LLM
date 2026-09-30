WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        COALESCE(brand, 'Unknown') AS brand_filled,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'new')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        brand_filled,
        resolved_brand,
        price + cost AS margin
    FROM valid
    WHERE launch_date > DATE '2023-06-05'
      AND brand IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        cost,
        brand_filled,
        margin,
        LEAD(cost) OVER w_running AS next_cost,
        LEAD(margin) OVER w_running AS next_margin,
        LAG(margin) OVER w_running AS prev_margin,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM cleaned
    WINDOW w_running AS (PARTITION BY brand ORDER BY price ASC, id ASC)
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand_filled,
        s.margin,
        s.next_cost,
        s.next_margin,
        s.seq_no,
        latest.order_item_quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_latest
    EXCEPT ALL
    SELECT id
    FROM products
    WHERE brand = 'Initech'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    brand_filled,
    margin,
    seq_no,
    order_item_quantity
FROM kept_rows
ORDER BY order_item_quantity DESC NULLS FIRST
