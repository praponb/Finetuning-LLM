WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS row_num
    FROM order_items
    QUALIFY row_num = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS dedup_rank
        FROM deduped
    ) ranked
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        row_num,
        FLOOR(quantity) AS quantity_whole
    FROM ranked
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach products attributes
    SELECT a.*, p.brand
    FROM cleaned a
    LEFT JOIN products p
        ON a.product_id = p.id
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    row_num,
    brand,
    CASE WHEN brand RLIKE '^(Initech|Globex)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS brand_zone
FROM joined
ORDER BY brand_zone NULLS FIRST, row_num;
