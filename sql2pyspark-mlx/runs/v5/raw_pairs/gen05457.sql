WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY category ORDER BY cost DESC NULLS FIRST) AS row_num
    FROM products
    QUALIFY row_num = 1
),
deduped AS (
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS LAST) = 1
),
not_flagged AS (
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock < s.launch_date
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, supplier_id ORDER BY row_num DESC, id) AS rn
        FROM not_flagged
    ) t
    WHERE rn = 1
),
filtered AS (
    SELECT
        *,
        CONCAT(brand, ', ', category) AS brand_category,
        stock_qty * cost AS stock_qty_times_cost,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM ranked
    WHERE ARRAY_CONTAINS(tags, 'clearance')
      AND supplier_id IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LEAD(cost, 1) OVER (PARTITION BY supplier_id ORDER BY stock_qty_times_cost, id) AS next_cost,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost
    FROM filtered
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            category,
            brand,
            price
        FROM enriched
    ) src
    PIVOT (
        AVG(price)
        FOR brand IN ('Acme' AS acme, 'Umbrella' AS umbrella)
    )
)
SELECT category, umbrella
FROM by_type
ORDER BY category NULLS LAST
