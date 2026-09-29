WITH latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.store_sale_revenue
    FROM stores s
    JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS dedup_rank
    FROM latest_match
    QUALIFY dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT region, city
    FROM ranked
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE city = 'Berlin'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.region = k.region AND s.city = k.city
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity = 183
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id IN (3, 4, 5)
        )
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        SUM(CASE WHEN city = 'New York' THEN store_sale_revenue ELSE 0 END) AS new_york_store_sale_revenue,
        ROUND(AVG(sqft), 2) AS avg_sqft,
        ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS sqft_per_new_york
    FROM eligible
    GROUP BY DATE_TRUNC('YEAR', opened_date)
    HAVING sqft_per_new_york > 0
)
SELECT
    year_start,
    new_york_store_sale_revenue,
    avg_sqft,
    sqft_per_new_york
FROM stats
ORDER BY year_start, sqft_per_new_york;
