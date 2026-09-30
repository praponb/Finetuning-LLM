WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        DATE_SUB(last_restock, 30) AS last_restock_minus_30d
    FROM inventory
    WHERE last_restock > TO_DATE('2023-05-01')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_minus_30d DESC, id) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.last_restock,
        src.last_restock_minus_30d,
        src.rn,
        p.store_name,
        p.opened_date
    FROM latest src
    LEFT JOIN stores p
        ON src.store_id = p.id
    WHERE p.opened_date BETWEEN '2024-01-01' AND '2025-03-17'
),
valid_inventory AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        last_restock,
        last_restock_minus_30d,
        rn,
        store_name,
        opened_date,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM joined
    WHERE UPPER(store_name) LIKE '%LD %'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        store_name,
        days_since_last_restock,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY days_since_last_restock NULLS FIRST) AS days_since_last_restock_cume_dist,
        MIN(days_since_last_restock) OVER (PARTITION BY store_id) AS store_id_min_days_since_last_restock,
        LAST_VALUE(days_since_last_restock, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_last_restock
    FROM valid_inventory
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        last_restock,
        store_name,
        days_since_last_restock,
        days_since_last_restock_cume_dist,
        store_id_min_days_since_last_restock,
        last_known_days_since_last_restock,
        UPPER(store_name) AS store_name_upper
    FROM scored
    WHERE last_restock > DATE '2023-06-03'
)
SELECT
    id,
    days_since_last_restock,
    days_since_last_restock_cume_dist,
    store_id_min_days_since_last_restock,
    last_known_days_since_last_restock,
    store_name_upper
FROM base
ORDER BY id DESC
LIMIT 10
