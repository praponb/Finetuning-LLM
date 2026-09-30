WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= '2024-03-01'
        )
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.price < 930
        )
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN rating >= 4.0 THEN 'High' WHEN rating >= 2.3 THEN 'Medium' ELSE 'Low' END AS rating_level,
        COALESCE(city, 'OTHER') AS city_filled
    FROM eligible_suppliers
    WHERE country = 'JP'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.rating,
        x.product_cost
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    supplier_name,
    product_cost,
    CASE WHEN supplier_name RLIKE '^(Pacific Wholesale|Global Supply)$' THEN 'GROUP_B' WHEN supplier_name RLIKE '^(Siam Parts Co|Tokyo Traders)$' THEN 'CORE' ELSE 'PRIORITY' END AS supplier_name_zone
FROM with_last
ORDER BY product_cost DESC NULLS LAST;
