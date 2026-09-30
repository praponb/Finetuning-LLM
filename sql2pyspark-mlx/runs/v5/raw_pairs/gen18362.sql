WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock > DATE '2023-07-07'
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        src.quantity_filled,
        s.opened_date
    FROM base src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.region,
        p.sqft,
        p.opened_date AS store_opened_date
    FROM joined e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined_inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft DESC, id) = 1
),
base_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        quantity_filled,
        opened_date,
        region,
        sqft,
        store_opened_date,
        quantity + sqft AS quantity_plus_sqft,
        GREATEST(sqft, quantity) AS greatest_sqft_quantity,
        COALESCE(region, 'NONE') AS resolved_region
    FROM latest
    WHERE opened_date <= '2023-03-13'
),
latest_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, region ORDER BY quantity_filled DESC NULLS FIRST, id) AS rank_in_group
    FROM base_inventory
    QUALIFY rank_in_group = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        quantity_filled,
        opened_date,
        quantity_plus_sqft,
        greatest_sqft_quantity,
        resolved_region,
        rank_in_group,
        CEIL(quantity) AS quantity_whole,
        ROUND(greatest_sqft_quantity * 0.85, 2) AS greatest_sqft_quantity_adjusted
    FROM latest_inventory
    WHERE opened_date > '2024-11-01'
)
SELECT
    id,
    store_id,
    quantity,
    opened_date,
    greatest_sqft_quantity,
    rank_in_group,
    greatest_sqft_quantity_adjusted
FROM valid
ORDER BY id DESC
