WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(city) AS city_upper
    FROM stores
    WHERE sqft < 7666
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        city_upper,
        REPLACE(store_name, ' ', '-') AS store_name_slug,
        CASE WHEN city IN ('New York', 'Singapore') THEN 'Tier 1' ELSE 'Other' END AS city_group,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM not_flagged
    WHERE opened_date >= DATE '2023-06-24'
      AND region IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        city_upper,
        store_name_slug,
        city_group,
        SUBSTRING(city, 1, 2) AS city_prefix,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM filtered
    WHERE opened_date >= DATE '2023-07-26'
      AND region IS NOT NULL
      AND region <> 'West'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        SUM(days_since_opened_date) FILTER (WHERE region = 'West') AS west_total_days_since_opened_date,
        AVG(days_since_opened_date) AS avg_days_since_opened_date,
        ROUND(SUM(days_since_opened_date) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS days_since_opened_date_per_new_york
    FROM valid
    GROUP BY city
    HAVING SUM(days_since_opened_date) FILTER (WHERE region = 'West') > 0
)
SELECT
    city,
    avg_days_since_opened_date,
    days_since_opened_date_per_new_york
FROM grouped
ORDER BY days_since_opened_date_per_new_york DESC NULLS LAST, city ASC
LIMIT 10;
