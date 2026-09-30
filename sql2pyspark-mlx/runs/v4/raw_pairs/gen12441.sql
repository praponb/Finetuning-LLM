WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        TRIM(region) clean_region,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END is_new_york,
        COALESCE(region, 'DIRECT') resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        clean_region,
        is_new_york,
        resolved_region,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY is_new_york DESC, id ASC) event_seq,
        CUME_DIST() OVER (PARTITION BY region, city ORDER BY is_new_york ASC) is_new_york_cume_dist,
        NTILE(5) OVER (PARTITION BY city ORDER BY is_new_york DESC, id ASC) is_new_york_bucket,
        LAG(is_new_york, 1) OVER (PARTITION BY city ORDER BY is_new_york ASC, id ASC) prev_is_new_york,
        AVG(is_new_york) OVER (ORDER BY is_new_york ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) is_new_york_avg_6_prior
    FROM cleaned
),
ranked AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY is_new_york_bucket DESC, id ASC) = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    clean_region,
    is_new_york,
    event_seq,
    is_new_york_cume_dist,
    is_new_york_bucket,
    CASE WHEN store_name RLIKE '^(Mall Central|Siam Flagship)$' THEN 'PRIORITY' WHEN store_name RLIKE '^(Downtown Express|Airport Kiosk)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS store_name_zone
FROM ranked
ORDER BY is_new_york_cume_dist ASC NULLS FIRST;
