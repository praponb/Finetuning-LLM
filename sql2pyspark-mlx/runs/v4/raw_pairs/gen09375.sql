WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2024-01-21')
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.sqft,
        d.opened_date,
        d.store_name
    FROM base s
    INNER JOIN stores d
        ON s.store_id = d.id
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.opened_date AS store_opened_date,
        p.city
    FROM joined src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined_inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST, id) = 1
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    sqft,
    store_name,
    store_opened_date,
    city
FROM ranked
ORDER BY last_restock DESC;
