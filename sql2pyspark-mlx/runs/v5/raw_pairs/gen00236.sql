WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (24, 34, 38)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        quantity,
        unit_price_int,
        LEAD(unit_price_int) OVER w_part AS next_unit_price_int,
        LEAD(quantity) OVER w_part AS next_quantity,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM valid
    WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity, id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(next_unit_price_int) AS next_unit_price_int_whole
    FROM with_windows
    WHERE product_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, p.brand
    FROM latest src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE p.stock_qty <= 207
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        next_unit_price_int,
        next_unit_price_int_whole,
        next_unit_price_int * row_seq AS next_unit_price_int_times_row_seq,
        LENGTH(brand) AS brand_length
    FROM enriched
    WHERE product_id IN (5, 21)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.price,
        p.brand,
        p.category
    FROM cleaned_order_items a
    LEFT JOIN products p
        ON a.product_id <=> p.id
)
SELECT
    id,
    next_unit_price_int_times_row_seq,
    brand,
    category,
    CASE WHEN brand RLIKE '^(Umbrella|Acme)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS brand_zone
FROM with_dims
ORDER BY category DESC;
