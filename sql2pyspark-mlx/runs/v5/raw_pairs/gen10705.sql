WITH with_dims AS (
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.supplier_id,
        s.country,
        s.supplier_name,
        s.city
    FROM products src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
not_flagged AS (
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.store_id = 1
        )
),
not_flagged_products AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IS NULL
        )
),
enriched AS (
    SELECT
        e.*,
        b.country AS supplier_country,
        b.supplier_name AS supplier_supplier_name,
        b.rating
    FROM not_flagged_products e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
    WHERE b.rating >= 4.0
),
remaining_keys AS (
    SELECT id, supplier_id
    FROM enriched
    INTERSECT ALL
    SELECT id, supplier_id
    FROM products
    WHERE cost >= 570
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.supplier_id = k.supplier_id
),
remaining_keys_2 AS (
    SELECT id
    FROM kept_rows
    INTERSECT
    SELECT id
    FROM products
    WHERE category = 'Fashion'
),
verified AS (
    SELECT s.*
    FROM kept_rows s
    JOIN remaining_keys_2 k ON s.id = k.id
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY rating DESC) AS rating_dense_rank,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
    FROM verified
)
SELECT *
FROM (
    SELECT
        city,
        supplier_id,
        country,
        rating_share
    FROM scored
) src
PIVOT (
    SUM(rating_share) AS amount,
    COUNT(1) AS n
    FOR country IN ('DE', 'US', 'JP')
)
