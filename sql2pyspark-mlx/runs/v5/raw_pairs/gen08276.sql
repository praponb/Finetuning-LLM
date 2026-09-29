WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SIZE(tags) AS tags_count
    FROM products
    WHERE brand IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Acme'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty NULLS FIRST, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.quantity = 6
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags_count,
        s.dedup_rank,
        latest.sale_date,
        latest.revenue
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        tags_count,
        sale_date,
        revenue
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY revenue DESC NULLS FIRST, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        tags_count,
        revenue,
        LEAD(revenue, 2) OVER (PARTITION BY category ORDER BY tags_count, id) AS next_revenue,
        NTILE(10) OVER (PARTITION BY category ORDER BY price DESC, id) AS price_bucket,
        SUM(cost) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM ranked
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, price_bucket + running_cost AS price_bucket_plus_running_cost
    FROM with_windows
    WHERE revenue > 1100
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    category,
    metric_name,
    value
FROM (
    SELECT
        id,
        category,
        revenue,
        next_revenue,
        price_bucket_plus_running_cost
    FROM base
) src
UNPIVOT (
    value FOR metric_name IN (revenue, next_revenue, price_bucket_plus_running_cost)
)
