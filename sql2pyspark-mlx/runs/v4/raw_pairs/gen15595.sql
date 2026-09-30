WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        last_restock,
        DATE_SUB(last_restock, 76) AS last_restock_minus_76d,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM inventory
    WHERE last_restock > TO_DATE('2023-09-10')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        last_restock,
        last_restock_minus_76d,
        quantity_str,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_str DESC, id) AS row_seq,
        FIRST_VALUE(quantity_str, TRUE) OVER (PARTITION BY store_id ORDER BY quantity_str, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_str,
        LEAD(quantity_str, 1) OVER w_ordered AS next_quantity_str
    FROM base
    WINDOW w_ordered AS (ORDER BY last_restock_minus_76d, id)
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.store_name,
        b.region
    FROM with_windows src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        b.city,
        b.opened_date
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        quantity_str,
        next_quantity_str
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (quantity_str AS QUANTITY_STR_VAL, next_quantity_str)
)
