WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC NULLS LAST, id) AS rank_in_group
    FROM products
    QUALIFY rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_name,
        category,
        cost,
        launch_date,
        tags,
        supplier_id,
        rank_in_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC NULLS LAST, id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost, 1, 0) OVER (PARTITION BY category, supplier_id ORDER BY cost, id) AS prev_cost,
        FIRST_VALUE(cost) IGNORE NULLS OVER w_ordered AS first_known_cost,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY rank_in_group DESC, id) AS event_seq
    FROM latest
    WINDOW w_ordered AS (PARTITION BY category, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.product_id IN (2, 7, 9)
        )
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.cost,
        s.launch_date,
        s.tags,
        s.prev_cost,
        s.first_known_cost,
        x.last_restock,
        x.quantity
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    product_name,
    cost,
    launch_date,
    tags,
    prev_cost,
    quantity
FROM latest_match
ORDER BY id DESC;
