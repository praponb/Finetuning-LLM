WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        ROUND(stock_qty, 1) AS stock_qty_rounded
    FROM products
    WHERE launch_date > DATE '2023-06-29'
),
filtered_products AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost / NULLIF(stock_qty_rounded, 0) AS cost_per_stock_qty_rounded,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM filtered
    WHERE supplier_id IS NOT NULL
      AND cost IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty ASC, id ASC) AS latest_rank
        FROM filtered_products
    ) x
    WHERE latest_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY cost DESC, id ASC) AS rank_in_group
    FROM deduped
    QUALIFY rank_in_group = 1
),
stats AS (
    -- Multi-level aggregation
    SELECT
        supplier_id,
        ROUND(AVG(cost), 2) AS avg_cost,
        BIT_OR(stock_qty) AS stock_qty_bit_or,
        COUNT_IF(category = 'Books') AS books_events,
        ROUND(SUM(cost_per_stock_qty_rounded) / NULLIF(COUNT(CASE WHEN category = 'Fashion' THEN 1 END), 0), 2) AS cost_per_stock_qty_rounded_per_fashion,
        APPROX_PERCENTILE(stock_qty, 0.95) AS stock_qty_p95
    FROM ranked
    GROUP BY supplier_id
    HAVING avg_cost < 1000000
)
SELECT supplier_id, stock_qty_bit_or
FROM stats
ORDER BY stock_qty_bit_or ASC NULLS LAST;
