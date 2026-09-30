WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock < s.launch_date
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        s.*,
        p2.country,
        p2.rating,
        p2.city
    FROM eligible s
    INNER JOIN suppliers p2
        ON s.supplier_id = p2.id
),
with_dims_products AS (
    -- Attach suppliers attributes
    SELECT
        a.*,
        p.rating AS supplier_rating,
        p.supplier_name,
        p.country AS supplier_country
    FROM with_dims a
    INNER JOIN suppliers p
        ON a.supplier_id = p.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN rating >= 3.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
        CASE WHEN stock_qty >= 301 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
        SUM(CASE WHEN city = 'New York' THEN price ELSE 0 END) AS new_york_price,
        SUM(CASE WHEN supplier_country = 'TH' THEN stock_qty ELSE 0 END) AS th_stock_qty,
        SIZE(COLLECT_SET(country)) AS num_countries,
        SUM(supplier_rating) FILTER (WHERE country = 'JP') AS jp_total_supplier_rating,
        RANK() OVER (PARTITION BY CASE WHEN rating >= 3.8 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN city = 'New York' THEN price ELSE 0 END) DESC) AS new_york_price_rank
    FROM with_dims_products
    GROUP BY rating_tier, stock_qty_tier
)
SELECT
    rating_tier,
    stock_qty_tier,
    new_york_price,
    th_stock_qty,
    num_countries,
    jp_total_supplier_rating,
    new_york_price_rank
FROM daily
ORDER BY rating_tier ASC;
