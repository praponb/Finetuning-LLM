WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        REPLACE(product_name, ' ', '') product_name_slug
    FROM products
    WHERE launch_date >= DATE '2023-05-12'
      AND cost < 190
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        product_name_slug,
        SUM(price) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_price,
        PERCENT_RANK() OVER (PARTITION BY brand ORDER BY cost NULLS FIRST) cost_percent_rank
    FROM filtered
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.quantity
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        b.country,
        b.supplier_name
    FROM latest_match e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            supplier_id,
            brand,
            cost_percent_rank
        FROM enriched
    ) src
    PIVOT (
        SUM(cost_percent_rank) AS amount,
        COUNT(1) AS n
        FOR brand IN ('Globex' AS globex, 'Acme' AS acme)
    )
)
SELECT
    supplier_id,
    globex_n,
    acme_n
FROM wide
