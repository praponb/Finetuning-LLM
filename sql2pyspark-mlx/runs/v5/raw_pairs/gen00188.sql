WITH base AS (
    SELECT
        *,
        GREATEST(cost, stock_qty) AS greatest_cost_stock_qty,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE launch_date > DATE '2023-09-02'
      AND brand IS NOT NULL
),
matched_keys AS (
    SELECT brand, id
    FROM base
    EXCEPT DISTINCT
    SELECT brand, id
    FROM products
    WHERE price > 390
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
),
enriched AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.brand_clean,
        p.rating,
        p.country,
        p.city
    FROM verified s
    INNER JOIN suppliers p
        ON s.supplier_id = p.id
),
scored AS (
    SELECT
        id,
        brand,
        price,
        cost,
        launch_date,
        tags,
        country,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
        DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY rating) AS rating_dense_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY stock_qty NULLS LAST) AS stock_qty_percent_rank
    FROM enriched
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (2, 9, 10)
        )
)
SELECT
    CASE WHEN cost_share >= 0.67 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_share_tier,
    country,
    ROUND(AVG(cost), 2) AS avg_cost,
    APPROX_PERCENTILE(price_share, 0.95) AS price_share_p95,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    AVG(stock_qty_percent_rank) AS avg_stock_qty_percent_rank,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN cost_share >= 0.67 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY ROUND(AVG(cost), 2) DESC) AS avg_cost_percent_rank
FROM not_flagged
GROUP BY CASE WHEN cost_share >= 0.67 THEN 'PREMIUM' ELSE 'STANDARD' END, country
ORDER BY price_share_p95
