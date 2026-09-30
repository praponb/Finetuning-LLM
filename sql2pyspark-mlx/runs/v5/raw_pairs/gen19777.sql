WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        quantity,
        last_restock
    FROM inventory
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity, 1) AS quantity_rounded,
        ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM latest
    WHERE last_restock > '2023-07-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM inventory
    WHERE quantity <= 70
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id
),
filtered_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        last_restock_plus_months,
        quantity_rounded - quantity AS quantity_rounded_minus_quantity
    FROM reconciled
    WHERE last_restock >= DATE '2023-07-20'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        PERCENT_RANK() OVER (ORDER BY quantity_rounded_minus_quantity DESC) AS quantity_rounded_minus_quantity_percent_rank,
        PERCENT_RANK() OVER (ORDER BY quantity_rounded_minus_quantity) AS quantity_rounded_minus_quantity_percent_rank_2,
        CUME_DIST() OVER (ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist,
        CUME_DIST() OVER (ORDER BY quantity_rounded_minus_quantity DESC NULLS FIRST) AS quantity_rounded_minus_quantity_cume_dist
    FROM filtered_inventory
    WHERE quantity IS NOT NULL
)
SELECT
    id,
    quantity,
    quantity_rounded_minus_quantity_percent_rank,
    quantity_rounded_minus_quantity_percent_rank_2
FROM with_windows
