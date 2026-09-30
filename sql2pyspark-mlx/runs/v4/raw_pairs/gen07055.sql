WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE store_id IN (1, 4, 5)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock NULLS FIRST, id) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        last_restock_month_end,
        CASE WHEN quantity >= 200 THEN 'High' WHEN quantity >= 101 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM ranked
    WHERE quantity >= 68
),
enriched AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.opened_date,
        p2.city
    FROM cleaned s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock_month_end,
        quantity_band,
        opened_date,
        city,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_month_end, id) AS seq_no,
        LAST_VALUE(city) IGNORE NULLS OVER w_running AS last_known_city,
        LAST_VALUE(city) IGNORE NULLS OVER w_ordered AS last_known_city_2
    FROM enriched
    WINDOW w_running AS (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY city ORDER BY last_restock_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    last_restock_month_end,
    quantity_band,
    opened_date,
    seq_no,
    last_known_city,
    last_known_city_2
FROM with_windows
ORDER BY id
LIMIT 10
