WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        SUBSTRING(region, 1, 4) region_prefix,
        CONCAT(store_name, '/', city) store_name_city,
        COALESCE(region, 'NONE') resolved_region
    FROM stores
    WHERE opened_date >= '2025-05-24'
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        region_prefix,
        store_name_city,
        resolved_region,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) event_seq,
        LAST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft,
        LEAD(sqft, 1, 0) OVER w_part next_sqft
    FROM valid
    WINDOW w_part AS (ORDER BY sqft, id)
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(next_sqft, TRUE) OVER (PARTITION BY city ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_next_sqft,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY city, region), 4) event_seq_share
    FROM scored
)
SELECT
    id,
    city,
    opened_date,
    sqft,
    region_prefix,
    store_name_city,
    resolved_region,
    event_seq,
    last_known_sqft,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS city_zone
FROM metrics
ORDER BY city DESC NULLS LAST;
