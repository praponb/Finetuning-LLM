WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        LENGTH(region) AS region_length,
        COALESCE(region, 'OTHER') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-10-26')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        region_length,
        region_clean,
        MIN(region_length) OVER (PARTITION BY city) AS city_min_region_length,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
    FROM base
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY region_length DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY city_min_region_length DESC, id) AS event_seq,
        LAG(city_min_region_length, 2, 0) OVER w_part AS prev_city_min_region_length,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY region_length, id) AS region_length_bucket
    FROM deduped
    WINDOW w_part AS (PARTITION BY city ORDER BY opened_date, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.quantity > 222
        )
),
enriched_stores AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        region_length,
        region_clean,
        city_min_region_length,
        first_known_region,
        event_seq,
        prev_city_min_region_length,
        region_length_bucket,
        NTILE(5) OVER (ORDER BY prev_city_min_region_length, id) AS prev_city_min_region_length_bucket,
        AVG(city_min_region_length) OVER (ORDER BY city_min_region_length, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS city_min_region_length_avg_3_prior,
        SUM(prev_city_min_region_length) OVER (PARTITION BY region) AS region_sum_prev_city_min_region_length
    FROM eligible
)
-- Grouped metrics
SELECT
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    ROUND(SUM(prev_city_min_region_length) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS prev_city_min_region_length_per_singapore,
    SUM(CASE WHEN region = 'South' THEN prev_city_min_region_length_bucket ELSE 0 END) AS south_prev_city_min_region_length_bucket,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS event_seq_per_london,
    GROUPING(city) AS grouping_id
FROM enriched_stores
GROUP BY ROLLUP (city)
HAVING SUM(CASE WHEN region = 'South' THEN prev_city_min_region_length_bucket ELSE 0 END) >= 0;
