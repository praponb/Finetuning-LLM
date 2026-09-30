WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(last_restock, 1) AS last_restock_plus_months,
        ROUND(quantity, 1) AS quantity_rounded
    FROM inventory
    WHERE last_restock >= DATE '2024-04-29'
),
joined AS (
    -- Attach stores attributes
    SELECT a.*, s.opened_date
    FROM cleaned a
    INNER JOIN stores s
        ON a.store_id = s.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity ASC NULLS LAST, id ASC) = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.category,
        p.brand
    FROM latest src
    LEFT JOIN products p
        ON src.product_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded ASC NULLS LAST, id ASC) AS dedup_rank
        FROM with_dims
    ) ranked
    WHERE dedup_rank = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, store_id ORDER BY quantity DESC, id ASC) AS rank_in_group
        FROM ranked
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    product_id,
    last_restock,
    quantity_rounded,
    brand,
    dedup_rank,
    rank_in_group,
    CASE WHEN brand RLIKE '^(Stark|Acme)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS brand_zone
FROM deduped
ORDER BY rank_in_group DESC, last_restock DESC;
