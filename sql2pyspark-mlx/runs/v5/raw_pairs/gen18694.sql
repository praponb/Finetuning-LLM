WITH filtered AS (
    SELECT
        *,
        COALESCE(cost, 0) cost_filled,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE price <= 630
),
enriched AS (
    SELECT
        src.*,
        p.rating,
        p.country
    FROM filtered src
    INNER JOIN suppliers p
        ON src.supplier_id <=> p.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price DESC, id ASC) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
remaining_keys AS (
    SELECT supplier_id
    FROM latest
    EXCEPT
    SELECT supplier_id
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'eco')
),
kept_rows AS (
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date >= s.launch_date
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date > '2024-12-01'
        )
),
base AS (
    SELECT *, ARRAY_JOIN(tags, '|') tags_text
    FROM eligible
    WHERE country IS NOT NULL
)
SELECT
    s.country AS supplier_country,
    s.city,
    COUNT(*) AS num_rows,
    ROUND(AVG(a.price), 2) AS avg_price,
    GROUPING_ID() AS level_id
FROM base a
INNER JOIN suppliers s ON a.supplier_id = s.id
GROUP BY GROUPING SETS ((s.country, s.city), (s.country))
ORDER BY avg_price ASC NULLS LAST
