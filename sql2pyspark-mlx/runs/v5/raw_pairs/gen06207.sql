WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date NULLS FIRST, id) AS row_num
    FROM stores
    QUALIFY row_num = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.opened_date
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CONCAT_WS('/', store_name, city) AS store_name_city_key,
        COALESCE(region, 'OTHER') AS region_clean
    FROM kept
    WHERE region IS NOT NULL
      AND sqft >= 8878
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.region,
        s.sqft,
        top1.amount
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 1700 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    region,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    MIN(sqft) AS min_sqft,
    SUM(amount) FILTER (WHERE city = 'Tokyo') AS tokyo_total_amount,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows
FROM with_latest
GROUP BY amount_tier, region
HAVING MIN(sqft) > 0
