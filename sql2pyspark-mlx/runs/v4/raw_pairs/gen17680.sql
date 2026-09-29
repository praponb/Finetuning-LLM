WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        UPPER(city) AS city_upper,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-10-02'
      AND region IS NOT NULL
),
metrics AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        FIRST_VALUE(sqft, TRUE) OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        MIN(sqft) OVER (PARTITION BY city) AS city_min_sqft
    FROM base
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date ASC, id ASC) AS seq_no,
        LEAD(city_min_sqft, 1, 0) OVER (PARTITION BY region ORDER BY sqft_share ASC, id ASC) AS next_city_min_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY first_known_sqft ASC, id ASC) AS row_seq
    FROM metrics
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
),
eligible_stores AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.product_id IN (1, 5, 9)
        )
)
SELECT
    city,
    region,
    SUM(CASE WHEN city = 'Singapore' THEN sqft ELSE 0 END) AS singapore_sqft,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    COUNT_IF(region = 'West') AS west_events,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY COUNT(CASE WHEN city = 'Singapore' THEN 1 END) DESC) AS singapore_count_percent_rank
FROM eligible_stores
GROUP BY city, region
