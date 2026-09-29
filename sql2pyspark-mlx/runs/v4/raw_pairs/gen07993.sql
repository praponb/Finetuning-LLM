WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE order_id IN (2, 6, 28, 39)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY unit_price_rounded DESC NULLS FIRST) = 1
),
joined AS (
    -- Attach products attributes
    SELECT src.*, d.category
    FROM deduped src
    LEFT JOIN products d
        ON src.product_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.stock_qty,
        b.cost,
        b.price
    FROM joined a
    INNER JOIN products b
        ON a.product_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cost >= 660 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    ROUND(AVG(cost), 2) AS avg_cost,
    SUM(CASE WHEN category = 'Sports' THEN unit_price ELSE 0 END) AS sports_unit_price,
    SUM(CASE WHEN category = 'Fashion' THEN price ELSE 0 END) AS fashion_price,
    COUNT(*) FILTER (WHERE category = 'Fashion') AS fashion_rows
FROM with_dims
GROUP BY CASE WHEN cost >= 660 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_cost >= 0
ORDER BY sports_unit_price DESC NULLS FIRST
