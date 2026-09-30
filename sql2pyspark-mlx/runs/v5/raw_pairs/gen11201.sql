WITH with_windows AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) sqft_share,
        LAG(sqft, 1) OVER w_grp prev_sqft
    FROM stores
    WINDOW w_grp AS (PARTITION BY city ORDER BY sqft ASC, id ASC)
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft_share,
    prev_sqft,
    CASE WHEN city RLIKE '^(Bangkok|Berlin)$' THEN 'EXTENDED' WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS city_zone
FROM with_windows
ORDER BY id ASC
