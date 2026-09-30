WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        region,
        opened_date,
        ADD_MONTHS(opened_date, 12) AS opened_date_plus_months,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        opened_date_plus_months,
        region_clean,
        CONCAT_WS('/', region, store_name) AS region_store_name_key,
        COALESCE(region, 'N/A') AS resolved_region
    FROM cleaned
    WHERE LENGTH(store_name) >= 6
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.shipping_fee < 36
        )
),
valid_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(opened_date_plus_months, 17) AS opened_date_plus_months_minus_17d,
        COALESCE(region, 'NONE') AS region_filled
    FROM kept
    WHERE region IS DISTINCT FROM 'West'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_plus_months_minus_17d, id) AS row_seq,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY opened_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_plus_months_minus_17d, id) AS event_seq,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date_plus_months_minus_17d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region_2
    FROM valid_stores
    WHERE MONTH(opened_date_plus_months_minus_17d) IN (3, 7, 11)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM scored
    EXCEPT ALL
    SELECT id, region
    FROM stores
    WHERE region NOT IN ('North', 'South', 'East')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.id = k.id AND s.region = k.region
)
-- Grouped metrics
SELECT
    region,
    DATE_TRUNC('MONTH', opened_date_plus_months) AS activity_month,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    GROUPING_ID() AS grouping_id
FROM kept_rows
GROUP BY GROUPING SETS ((region, DATE_TRUNC('MONTH', opened_date_plus_months)), (region), ())
HAVING avg_event_seq < 1000000
ORDER BY avg_row_seq DESC;
