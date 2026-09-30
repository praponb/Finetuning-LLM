WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        ROUND(quantity, 1) quantity_rounded,
        ROUND(unit_price, 0) unit_price_rounded
    FROM order_items
    WHERE order_id IN (11, 23)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, quantity_rounded / NULLIF(unit_price_rounded, 0) quantity_rounded_per_unit_price_rounded
    FROM base
    WHERE unit_price_rounded > 22
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_rounded DESC, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        unit_price_rounded,
        quantity_rounded_per_unit_price_rounded,
        latest_rank,
        CEIL(quantity_rounded) quantity_rounded_whole,
        ROUND(quantity_rounded_per_unit_price_rounded, 1) quantity_rounded_per_unit_price_rounded_rounded
    FROM latest
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    unit_price_rounded,
    quantity_rounded_per_unit_price_rounded,
    latest_rank,
    quantity_rounded_whole
FROM cleaned_order_items;
