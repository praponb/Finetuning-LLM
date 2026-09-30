WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(store_name) AS store_name_upper,
        LOWER(city) AS city_lower
    FROM stores
    WHERE opened_date >= DATE '2023-04-18'
      AND UPPER(TRIM(store_name)) = 'OLD TOWN'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        store_name_upper,
        city_lower,
        CASE WHEN city IN ('Chiang Mai', 'Tokyo') THEN 'Tier 1' ELSE 'Other' END AS city_group,
        CASE WHEN city = 'London' THEN 1 ELSE 0 END AS is_london
    FROM cleaned
    WHERE opened_date >= DATE '2023-08-23'
      AND region IS NOT NULL
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.sqft,
        s.city_group,
        top1.sale_date,
        top1.store_sale_revenue
    FROM base s
    JOIN LATERAL (
            SELECT c.sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.channel IN ('web', 'partner')
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT city, region
    FROM kept
    INTERSECT
    SELECT city, region
    FROM stores
    WHERE sqft BETWEEN 6874 AND 8523
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.city = k.city AND s.region = k.region
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        store_name,
        city,
        sqft,
        city_group,
        store_sale_revenue,
        CUME_DIST() OVER (ORDER BY sqft NULLS LAST) AS sqft_cume_dist,
        RANK() OVER (PARTITION BY city, region ORDER BY store_sale_revenue DESC NULLS FIRST) AS store_sale_revenue_rank
    FROM verified
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        store_name,
        sqft,
        city_group,
        store_sale_revenue,
        CAST(store_sale_revenue AS INT) AS store_sale_revenue_int
    FROM metrics
    WHERE store_sale_revenue < 2500
)
SELECT
    sqft,
    city_group,
    store_sale_revenue_int
FROM valid;
