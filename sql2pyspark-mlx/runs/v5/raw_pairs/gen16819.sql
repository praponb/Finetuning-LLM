WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST, id) AS latest_rank
        FROM stores
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    city,
    sqft,
    CASE WHEN city RLIKE '^(Singapore|Chiang Mai)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'CORE' ELSE 'PRIORITY' END AS city_zone
FROM ranked
ORDER BY city_zone DESC;
