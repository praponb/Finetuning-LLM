WITH base AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE last_restock >= DATE '2023-08-18'
),
valid AS (
    SELECT
        id,
        product_id,
        quantity,
        ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM base
    WHERE product_id IN (2, 5, 6)
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_plus_months DESC NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    quantity,
    last_restock_plus_months
FROM latest
ORDER BY id DESC NULLS LAST;
