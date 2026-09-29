WITH latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_date
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        city
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY order_date DESC NULLS LAST, id) AS dedup_rank
        FROM latest_match
    ) x
    WHERE dedup_rank = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date BETWEEN '2023-07-26' AND '2023-12-01'
        )
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        top1.quantity
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity, 1) OVER (ORDER BY quantity, id) AS next_quantity,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS event_seq
    FROM with_last
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(quantity, next_quantity) AS least_quantity_next_quantity
    FROM scored
    WHERE next_quantity <= 100
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock BETWEEN '2023-07-04' AND '2025-01-01'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_quantity >= 237 THEN 'LARGE' ELSE 'SMALL' END AS next_quantity_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(least_quantity_next_quantity, ARRAY(0.25, 0.5, 0.75)) AS least_quantity_next_quantity_quartiles,
    APPROX_PERCENTILE(next_quantity, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_quartiles,
    APPROX_PERCENTILE(next_quantity, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_quartiles_2,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM kept
GROUP BY 1
