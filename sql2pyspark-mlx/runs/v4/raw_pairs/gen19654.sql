WITH latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.last_restock
    FROM products s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT category, brand
    FROM latest_match
    EXCEPT
    SELECT category, brand
    FROM products
    WHERE product_name LIKE '%Coff%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN kept_keys k ON s.category = k.category AND s.brand = k.brand
),
matched_keys AS (
    -- Set intersection on keys
    SELECT brand, id
    FROM reconciled
    INTERSECT
    SELECT brand, id
    FROM products
    WHERE product_name LIKE '%Chair%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.supplier_name,
        p.rating
    FROM kept_rows src
    INNER JOIN suppliers p
        ON src.supplier_id = p.id
)
SELECT
    id,
    product_name,
    brand,
    launch_date,
    tags,
    last_restock,
    supplier_name,
    rating
FROM joined
ORDER BY id DESC NULLS LAST, last_restock DESC
