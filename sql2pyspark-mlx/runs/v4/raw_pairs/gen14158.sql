WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN quantity >= 195 THEN 'High' WHEN quantity >= 80 THEN 'Medium' ELSE 'Low' END quantity_band
    FROM inventory
    WHERE product_id IN (4, 5, 7, 8)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(last_restock, 1) last_restock_plus_months,
        CASE WHEN quantity >= 193 THEN 'High' WHEN quantity >= 76 THEN 'Medium' ELSE 'Low' END quantity_band_2
    FROM valid
    WHERE last_restock > TO_DATE('2023-12-05')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY last_restock_plus_months ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity,
        LAG(quantity, 1) OVER (ORDER BY last_restock_plus_months ASC, id ASC) prev_quantity,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY last_restock_plus_months ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity_2,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock_plus_months ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) quantity_avg_6
    FROM cleaned
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_band,
        last_restock_plus_months,
        quantity_band_2,
        last_known_quantity,
        prev_quantity,
        last_known_quantity_2,
        quantity_avg_6,
        AVG(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_known_quantity ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_avg_3,
        LEAD(last_known_quantity, 1) OVER (PARTITION BY product_id ORDER BY last_known_quantity ASC, id ASC) next_last_known_quantity,
        FIRST_VALUE(last_known_quantity_2) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_last_known_quantity_2,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_known_quantity_2 ASC, id ASC) seq_no
    FROM enriched
)
SELECT
    id,
    store_id,
    last_restock,
    last_restock_plus_months,
    quantity_band_2,
    prev_quantity,
    last_known_quantity_2,
    quantity_avg_6,
    seq_no
FROM enriched_inventory
ORDER BY last_known_quantity_2 DESC NULLS LAST, prev_quantity DESC;
