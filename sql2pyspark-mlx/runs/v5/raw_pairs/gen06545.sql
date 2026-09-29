WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', supplier_name, country) supplier_name_country_key,
        COALESCE(city, 'N/A') city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.city_filled,
        top1.cost,
        top1.product_stock_qty
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM with_last
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE rating <= 2.6
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM reconciled
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE country = 'TH'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        CASE WHEN cost >= 580 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
        APPROX_PERCENTILE(product_stock_qty, 0.5) AS product_stock_qty_p50,
        SUM(CASE WHEN city = 'Tokyo' THEN product_stock_qty ELSE 0 END) AS tokyo_product_stock_qty,
        SIZE(COLLECT_SET(city)) AS num_cities,
        RANK() OVER (PARTITION BY city ORDER BY APPROX_PERCENTILE(product_stock_qty, 0.5) DESC) AS product_stock_qty_p50_rank
    FROM kept_rows
    GROUP BY city, CASE WHEN cost >= 580 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING tokyo_product_stock_qty < 1000000
)
SELECT
    city,
    cost_tier,
    product_stock_qty_p50,
    tokyo_product_stock_qty
FROM daily
ORDER BY city DESC NULLS LAST
