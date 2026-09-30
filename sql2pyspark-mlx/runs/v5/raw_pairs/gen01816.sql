WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, MONTH(launch_date) AS launch_date_month
    FROM products
    WHERE supplier_id IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id = 31
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT brand, supplier_id
    FROM eligible
    EXCEPT ALL
    SELECT brand, supplier_id
    FROM products
    WHERE cost >= 320
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
),
scored AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (PARTITION BY category ORDER BY cost NULLS FIRST) AS cost_rank
    FROM reconciled
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        a.id,
        a.product_name,
        a.price,
        a.tags,
        a.supplier_id,
        a.launch_date_month,
        b.country,
        b.rating,
        b.city
    FROM scored a
    LEFT JOIN suppliers b
        ON a.supplier_id <=> b.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (14, 19, 27, 35)
        )
),
enriched AS (
    -- Attach suppliers attributes
    SELECT s.*, p.supplier_name
    FROM not_flagged s
    LEFT JOIN suppliers p
        ON s.supplier_id = p.id
    WHERE p.rating >= 2.1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN launch_date_month >= 64 THEN 'LARGE' ELSE 'SMALL' END AS launch_date_month_tier,
    APPROX_PERCENTILE(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    COUNT(*) AS num_rows,
    ROUND(SUM(launch_date_month) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS launch_date_month_per_us,
    COUNT_IF(country = 'TH') AS th_events,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY ROLLUP (CASE WHEN launch_date_month >= 64 THEN 'LARGE' ELSE 'SMALL' END)
